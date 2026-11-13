//! A connection to a broker: TCP, TLS if set, the versions checked (ApiVersions), SASL if set.
//! Blocking, with timeouts, for requests and their answers in turn (`call`); or, once made,
//! non-blocking for the consumer's Fetch on the data thread (`Stream` as `Read`/`Write`).
use crate::config::Net;
use crate::error::{Code, Error, Result};
use crate::sasl::{self, Mechanism};
use crate::wire;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// A socket, in TLS or not.
pub enum Stream {
    Plain(Sock),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, Sock>>),
}

/// A TCP socket whose every read and write waits only the time left to `deadline` (none: as the
/// socket is set). Under TLS this is what bounds a request: rustls reads the socket as often as a
/// record needs, and a timeout set once would be waited again at each read, so a broker trickling
/// a byte at a time would hold a request (or the handshake) forever. At the deadline, WouldBlock.
pub struct Sock {
    tcp: TcpStream,
    deadline: Option<Instant>,
}

impl Sock {
    fn left(&self) -> std::io::Result<Option<Duration>> {
        let Some(d) = self.deadline else { return Ok(None) };
        let left = d.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(ErrorKind::WouldBlock.into());
        }
        Ok(Some(left))
    }
}

impl Read for Sock {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(left) = self.left()? {
            self.tcp.set_read_timeout(Some(left))?;
        }
        self.tcp.read(buf)
    }
}

impl Write for Sock {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(left) = self.left()? {
            self.tcp.set_write_timeout(Some(left))?;
        }
        self.tcp.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.tcp.flush()
    }
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Stream::Plain(_) => "Stream::Plain",
            Stream::Tls(_) => "Stream::Tls",
        })
    }
}

impl Stream {
    fn sock(&mut self) -> &mut Sock {
        match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => &mut s.sock,
        }
    }
    pub fn set_nonblocking(&mut self, on: bool) -> Result<()> {
        let s = self.sock();
        s.deadline = None;
        Ok(s.tcp.set_nonblocking(on)?)
    }
}

/// A stream whose reads and writes end at a deadline, each socket operation waiting only the
/// time left: a broker trickling its answer (or reading a request a byte at a time) cannot hold a
/// blocking request past its timeout. At the deadline, WouldBlock: `read_frame` and `write_all`
/// give it back.
struct Until<'a>(&'a mut Stream, Instant);

impl Read for Until<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.sock().deadline = Some(self.1);
        self.0.read(buf)
    }
}

impl Write for Until<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.sock().deadline = Some(self.1);
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.sock().deadline = Some(self.1);
        self.0.flush()
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }
    /// What TLS holds of what was written, sent (WouldBlock: sent in part, call again).
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => {
                while s.conn.wants_write() {
                    s.conn.write_tls(&mut s.sock)?;
                }
                Ok(())
            }
        }
    }
}

/// A connected, checked, authenticated broker connection, its requests answered in turn.
pub struct Conn {
    pub stream: Stream,
    pub addr: String,
    client_id: String,
    corr: i32,
    /// the last request, its buffer kept for the next (zeroed once SASL is through: it held the
    /// password)
    out: Vec<u8>,
    timeout: Duration,
    /// while connecting, the deadline every step shares
    until: Option<Instant>,
    max_response: usize,
    /// the broker's versions (key, min, max), as ApiVersions said
    apis: Vec<(i16, i16, i16)>,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never the buffers: the last request may be SASL's, with the password
        f.debug_struct("Conn").field("stream", &self.stream).field("addr", &self.addr).finish_non_exhaustive()
    }
}

/// A response's frame: the size first (up to `max`), then the frame itself into `buf`; Ok(false)
/// while it is not whole on a non-blocking socket (call again with the same `buf`).
pub fn read_frame(s: &mut impl Read, buf: &mut Vec<u8>, max: usize) -> std::io::Result<bool> {
    loop {
        let want = match buf.len() {
            n if n < 4 => 4,
            _ => {
                let n = i32::from_be_bytes(buf[..4].try_into().expect("4 bytes"));
                if n < 4 || n as usize > max {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        format!("a response of {n} bytes (receive.message.max.bytes is {max})"),
                    ));
                }
                4 + n as usize
            }
        };
        if buf.len() == want {
            return Ok(true);
        }
        let at = buf.len();
        // what the socket holds, up to a megabyte a read
        buf.resize(want.min(at + (1 << 20)), 0);
        match s.read(&mut buf[at..]) {
            Ok(0) => {
                buf.truncate(at);
                return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "the broker closed the connection"));
            }
            Ok(n) => buf.truncate(at + n),
            Err(e) => {
                buf.truncate(at);
                match e.kind() {
                    ErrorKind::Interrupted => {}
                    ErrorKind::WouldBlock => return Ok(false),
                    _ => return Err(e),
                }
            }
        }
    }
}

/// Writes `b` whole; Ok(false) where a non-blocking socket took less (`*written` says how much).
pub fn write_all(s: &mut impl Write, b: &[u8], written: &mut usize) -> std::io::Result<bool> {
    while *written < b.len() {
        match s.write(&b[*written..]) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => *written += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
            Err(e) => return Err(e),
        }
    }
    match s.flush() {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(e) => Err(e),
    }
}

impl Conn {
    /// Connects to `addr` (host:port), within the connection timeout, then TLS, the versions and
    /// SASL; each step's failure says which it is.
    pub fn open(net: &Net, addr: &str) -> Result<Conn> {
        Conn::open_within(net, addr, net.connect_timeout)
    }

    /// `open`, within `limit` (and the connection timeout): one deadline for every step,
    /// resolving the name, each address tried, TLS, ApiVersions and SASL each given what is left.
    pub fn open_within(net: &Net, addr: &str, limit: Duration) -> Result<Conn> {
        let limit = limit.min(net.connect_timeout).max(Duration::from_millis(1));
        let deadline = deadline_in(limit);
        let tcp = tcp(addr, deadline)?;
        tcp.set_nodelay(true)?;
        let sock = Sock { tcp, deadline: Some(deadline) };
        let stream = match &net.tls {
            None => Stream::Plain(sock),
            Some(cfg) => {
                let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h).trim_start_matches('[').trim_end_matches(']');
                let conn = rustls::ClientConnection::new(cfg.clone(), crate::tls::server_name(host)?)
                    .map_err(|e| Error::Auth(format!("{addr}: TLS: {e}")))?;
                let mut s = rustls::StreamOwned::new(conn, sock);
                while s.conn.is_handshaking() {
                    s.conn.complete_io(&mut s.sock).map_err(|e| match e.kind() {
                        ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                            Error::Timeout(format!("{addr}: the TLS handshake"))
                        }
                        _ => tls_error(addr, e),
                    })?;
                }
                Stream::Tls(Box::new(s))
            }
        };
        let mut c = Conn {
            stream,
            addr: addr.to_string(),
            client_id: net.client_id.clone(),
            corr: 0,
            out: vec![],
            timeout: net.request_timeout,
            until: Some(deadline),
            max_response: net.max_response,
            apis: vec![],
        };
        c.check_versions()?;
        if let Some((mech, user, pass)) = &net.sasl {
            let r = c.authenticate(*mech, user, pass);
            c.out.fill(0); // the last request held the password
            r?;
        }
        c.until = None;
        Ok(c)
    }

    fn check_versions(&mut self) -> Result<()> {
        let frame = self.call(wire::API_VERSIONS, |_| {})?;
        let (code, apis) = wire::api_versions(wire::Cur::new(&frame))?;
        if code != Code::NONE {
            return Err(Error::Unsupported(format!("{}: ApiVersions: {code}", self.addr)));
        }
        let missing: Vec<String> = (wire::VERSIONS.iter())
            .filter(|(key, ..)| wire::negotiate(*key, &apis).is_none())
            .map(|(key, lo, hi, name)| {
                let ours = if lo == hi { format!("v{lo}") } else { format!("v{lo}-v{hi}") };
                match apis.iter().find(|a| a.0 == *key) {
                    Some((_, min, max)) => format!("{name} {ours} (it speaks v{min}-v{max})"),
                    None => format!("{name} {ours} (it speaks none)"),
                }
            })
            .collect();
        if !missing.is_empty() {
            return Err(Error::Unsupported(format!(
                "{}: the broker does not speak {} (this client speaks Kafka 2.4 and later, Redpanda)",
                self.addr,
                missing.join(", ")
            )));
        }
        self.apis = apis;
        Ok(())
    }

    /// The version of `api` this connection speaks: the newest of this client's the broker speaks.
    pub fn version(&self, api: i16) -> i16 {
        wire::negotiate(api, &self.apis).unwrap_or_else(|| wire::version(api))
    }

    fn authenticate(&mut self, mech: Mechanism, user: &str, pass: &str) -> Result<()> {
        let addr = self.addr.clone();
        let frame = self.call(wire::SASL_HANDSHAKE, |o| wire::sasl_handshake_request(o, mech.name()))?;
        let (code, offered) = wire::sasl_handshake(wire::Cur::new(&frame))?;
        if code != Code::NONE {
            return Err(Error::Auth(format!(
                "{addr}: SASL {}: {code} (the broker offers {})",
                mech.name(),
                offered.join(", ")
            )));
        }
        let mut step = |auth: &[u8]| -> Result<Vec<u8>> {
            let frame = self.call(wire::SASL_AUTHENTICATE, |o| wire::sasl_authenticate_request(o, auth))?;
            let (code, msg, bytes) = wire::sasl_authenticate(wire::Cur::new(&frame))?;
            if code != Code::NONE {
                return Err(Error::Auth(format!("{addr}: SASL {}: {code}: {}", mech.name(), msg.unwrap_or_default())));
            }
            Ok(bytes)
        };
        match mech {
            Mechanism::Plain => drop(step(&sasl::plain(user, pass))?),
            _ => {
                let mut s = sasl::Scram::new(mech, user, pass)?;
                let server_first = step(&s.first())?;
                let fin = s.fin(&server_first).map_err(|e| Error::Auth(format!("{addr}: {e}")))?;
                let server_final = step(&fin)?;
                s.verify(&server_final).map_err(|e| Error::Auth(format!("{addr}: {e}")))?;
            }
        }
        Ok(())
    }

    /// Sends a request of `api` and waits for its answer: the response after its correlation id.
    pub fn call(&mut self, api: i16, body: impl FnOnce(&mut Vec<u8>)) -> Result<Vec<u8>> {
        self.corr = self.corr.wrapping_add(1);
        let corr = self.corr;
        let mut out = std::mem::take(&mut self.out);
        let v = if api == wire::API_VERSIONS { 0 } else { self.version(api) };
        wire::request_at(&mut out, api, v, corr, &self.client_id, body);
        let r = self.round_trip(&out, corr);
        self.out = out;
        r
    }

    fn round_trip(&mut self, out: &[u8], corr: i32) -> Result<Vec<u8>> {
        let addr = &self.addr;
        let timeout = |e: std::io::Error| match e.kind() {
            ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                Error::Timeout(format!("{addr}: no answer in {:?}", self.timeout))
            }
            _ => tls_error(addr, e),
        };
        let deadline = deadline_in(self.timeout);
        let deadline = self.until.map_or(deadline, |u| deadline.min(u)); // connecting: its deadline too
        let mut s = Until(&mut self.stream, deadline);
        let mut written = 0;
        while !write_all(&mut s, out, &mut written).map_err(timeout)? {
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!("{addr}: sending a request")));
            }
        }
        let mut buf = vec![];
        while !read_frame(&mut s, &mut buf, self.max_response).map_err(timeout)? {
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!("{addr}: no answer in {:?}", self.timeout)));
            }
        }
        wire::response(&buf[4..], corr)?;
        buf.drain(..8); // its size and correlation id
        Ok(buf)
    }

    /// The time left for one request.
    pub fn set_request_timeout(&mut self, t: Duration) {
        self.timeout = t;
    }

    /// Its socket, for requests sent and answered without blocking (the consumer's Fetch).
    pub fn into_stream(mut self) -> Result<Stream> {
        self.stream.set_nonblocking(true)?;
        Ok(self.stream)
    }
}

/// Now and `t`; a `t` past what the clock can count (`Duration::MAX`: no limit), a century.
pub(crate) fn deadline_in(t: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(t).or_else(|| now.checked_add(Duration::from_secs(100 * 365 * 86_400))).unwrap_or(now)
}

fn left(deadline: Instant, addr: &str) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| Error::Timeout(format!("{addr}: connecting (socket.connection.setup.timeout.ms)")))
}

/// An I/O error of a connection: TLS refusing a certificate (the broker's, or the broker the
/// client's) is Auth, which a retry would meet again; anything else, a listener not speaking TLS
/// among them, is Transport, which another broker, or a later try, may not meet.
fn tls_error(addr: &str, e: std::io::Error) -> Error {
    use rustls::AlertDescription as A;
    let tls = e.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>());
    let refused = match tls {
        Some(rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented) => true,
        Some(rustls::Error::AlertReceived(a)) => matches!(
            a,
            A::BadCertificate
                | A::UnsupportedCertificate
                | A::CertificateRevoked
                | A::CertificateExpired
                | A::CertificateUnknown
                | A::UnknownCA
                | A::AccessDenied
                | A::CertificateRequired
        ),
        _ => false,
    };
    match refused {
        true => Error::Auth(format!("{addr}: TLS: {e}")),
        false if tls.is_some() => Error::Transport(format!("{addr}: TLS: {e}")),
        false => Error::Transport(format!("{addr}: {e}")),
    }
}

/// `addr`'s addresses, by `deadline`. The system's resolver blocks as long as it likes, so it is
/// asked on a thread of its own, left to finish alone if the deadline comes first.
fn resolve(addr: &str, deadline: Instant) -> Result<Vec<SocketAddr>> {
    if let Ok(a) = addr.parse::<SocketAddr>() {
        return Ok(vec![a]);
    }
    let (tx, rx) = mpsc::channel();
    let name = addr.to_string();
    std::thread::Builder::new()
        .name("fastka-resolve".into())
        .spawn(move || drop(tx.send(name.to_socket_addrs().map(|a| a.collect::<Vec<_>>()))))
        .map_err(|e| Error::Transport(format!("a thread to resolve {addr}: {e}")))?;
    match rx.recv_timeout(left(deadline, addr)?) {
        Ok(r) => r.map_err(|e| Error::Transport(format!("{addr}: resolving: {e}"))),
        Err(_) => Err(Error::Timeout(format!("{addr}: resolving (socket.connection.setup.timeout.ms)"))),
    }
}

/// TCP to the first of `addr`'s addresses that answers by `deadline`, each given the time left.
fn tcp(addr: &str, deadline: Instant) -> Result<TcpStream> {
    let mut last = Error::Transport(format!("{addr}: resolves to no address"));
    for a in resolve(addr, deadline)? {
        let Ok(left) = left(deadline, addr) else { break }; // the last address's error said
        match TcpStream::connect_timeout(&a, left) {
            Ok(s) => return Ok(s),
            Err(e) => last = Error::Transport(format!("{addr} ({a}): connecting: {e}")),
        }
    }
    Err(last)
}

/// `Conn::open` on a thread of its own: resolving a name and connecting block, and the data
/// thread must not wait on either (a resolver that does not answer takes seconds per attempt).
pub fn open_in_background(net: std::sync::Arc<Net>, addr: String) -> mpsc::Receiver<Result<Conn>> {
    let (tx, rx) = mpsc::channel();
    let spawned = std::thread::Builder::new().name("fastka-connect".into()).spawn(move || {
        let _ = tx.send(Conn::open(&net, &addr));
    });
    if let Err(e) = spawned {
        let (tx, rx2) = mpsc::channel();
        let _ = tx.send(Err(Error::Transport(format!("a thread to connect: {e}"))));
        return rx2;
    }
    rx
}

#[cfg(test)]
mod tests;
