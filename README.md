# fastka

A Kafka client for Rust that is synchronous and pure Rust: no librdkafka, no OpenSSL, no async
runtime. The consumer fetches on your thread, and the producer is idempotent.

```toml
[dependencies]
fastka = "0.1"
```

fastka was written for one job: a market-data pipeline engine whose data thread used to spend
more time inside librdkafka than decoding the messages. It came out of that engine (where it
replaced librdkafka in production) as a crate that knows nothing of it. That origin explains both
what it is good at and the things it deliberately leaves out.

<!-- BENCH-SUMMARY -->

## What it is

- **A consumer that does its work on the thread that calls it.**
  - Each partition leader's Fetch is sent from your thread on a non-blocking socket.
  - The next Fetch is sent *before* the current response is parsed, so the broker prepares it in
    the meantime.
  - Records are read where they lie in the response buffer.
  - No thread hands messages to another, and there are no op queues or locks on the data path.
  - Connecting (DNS, TCP, TLS, SASL) and looking up leaders and offsets run on short-lived helper
    threads, so a dropped SYN never blocks your loop.
- **An idempotent producer** with delivery reports, and the semantics of librdkafka where they matter:
  - ordering per partition: one batch of a partition in flight, and a retry is sent with the same
    sequence number;
  - `message.timeout.ms` bounds a message from `send` to its report, retries and the request in
    flight included;
  - partitioning by key with Java's murmur2, so keyed records land where the Java client puts
    them; sticky partitioning (KIP-480) for keyless records;
  - each report says whether a failed message may still have been written (`Persisted::Possibly`).
- **A control plane:**
  - metadata and watermarks;
  - offsets for timestamps;
  - topic configs;
  - topic creation (idempotent across a lost answer);
  - offset commits for a group.
- **TLS** through rustls: a CA given by file or PEM, otherwise the system roots; client
  certificates.
- **SASL**: PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512.
- **Settings use librdkafka's names** (`bootstrap.servers`, `linger.ms`, `fetch.wait.max.ms`, …),
  so an existing deployment's `key=value` lines mean what they meant. A key the client doesn't
  honour is **refused at startup**, never silently ignored. A TLS setting dropped silently is how
  production ends up in plaintext.
- **Small.** About 6k lines of library code, with 9 dependencies (rustls, ring, the codecs and
  crc32c). zstd is the only C code it builds.

## What it is not

Read this before adopting it. Each item is a deliberate scope decision, and each one is refused
loudly at startup, not half-supported:

| Not supported | What to use instead |
|---|---|
| **Consumer groups** (subscribe, rebalancing, partition assignment by the broker) | Assign partitions yourself (`Assignment`). You can still commit offsets to a group with `Client::commit`. |
| **Transactions** (transactional producer, exactly-once across topics) | rdkafka. fastka *reads* transactional topics correctly with `read_committed`. |
| **async/await** | It is synchronous by design. In an async program, run it on a dedicated thread (`spawn_blocking`), or use rskafka. |
| **GSSAPI/Kerberos, OAUTHBEARER, encrypted private keys, JKS/PKCS#12** | rdkafka. |
| **Flexible protocol versions (KIP-482 tagged fields), Fetch sessions (KIP-227), leader epochs (KIP-320), SASL re-authentication (KIP-368)** | Every broker from Kafka 2.4 to 4.1, and Redpanda, still speaks the versions used here. A broker that drops them is refused at connect, naming the API. |
| **Epoch bump on a sequence gap** | Where librdkafka bumps the producer epoch and renumbers, a fastka producer becomes *fatal* (`fatal_error()`), and you create a new one. This only happens when a batch may have been written but its answer was lost. |

Two further limits. fastka has not been run by anyone except its authors, and its API is 0.x. The
verification below is thorough, but it doesn't replace years of other people's production traffic,
which librdkafka has.

## Use

### Consume

```rust,no_run
use fastka::consumer::{Assignment, Consumer, Offset, Polled};
use std::time::Duration;

let mut cfg = fastka::Config::new();
cfg.set("bootstrap.servers", "localhost:9092").set("enable.partition.eof", "true");
let mut consumer = Consumer::from_config(&cfg, vec![
    Assignment::new("trades", 0, Offset::Beginning),
    Assignment::new("trades", 1, Offset::At(42)),
])?;
loop {
    match consumer.next_event(Duration::from_millis(100), &mut |notice| eprintln!("{notice}"))? {
        Some((i, Polled::Msg(m))) => println!("partition #{i} @{}: {:?}", m.offset, m.value),
        Some((i, Polled::Eof)) => println!("partition #{i} caught up"),
        _ => {}
    }
}
# Ok::<(), fastka::Error>(())
```

`next_event` sends and reads Fetches without blocking and waits at most `timeout`. The callback
receives what the consumer recovered from by itself: a reconnect, a moved leader, or an offset
reset by `auto.offset.reset`. Count and log these. An `Err` is something a retry cannot fix (for
example, an offset out of range with `auto.offset.reset=error`). After an `Err` the consumer is
still usable, and no record has been skipped; `seek` moves a partition on.

### Produce

```rust,no_run
use fastka::producer::{Deliveries, Delivery, Producer, Record};
use std::time::Duration;

struct Reports;
impl Deliveries for Reports {
    type Opaque = u64; // anything you want back with the report
    fn delivery(&self, d: &Delivery<'_>, id: u64) {
        match d.error {
            None => println!("#{id} at {:?}/{:?}", d.partition, d.offset),
            Some(e) => eprintln!("#{id} failed: {e} (persisted: {:?})", d.persisted),
        }
    }
}

let mut cfg = fastka::Config::new();
cfg.set("bootstrap.servers", "localhost:9092").set("compression.type", "lz4");
let producer = Producer::new(&cfg, Reports)?;
producer.send(Record::to("trades").key(b"BTC-USD").value(b"{\"px\":97000}"), 1).map_err(|(e, _)| e)?;
producer.poll(Duration::ZERO); // hands over the reports ready, on this thread
producer.flush(Duration::from_secs(30))?;
# Ok::<(), fastka::Error>(())
```

`send` queues the record and returns at once. Background threads, one per broker, send each
partition's batches. `poll` and `flush` call your `delivery` on the thread that calls them. When
the queue is full, `send` returns `QueueFull` along with your opaque: poll, then send again.

## How it is verified

Every claim below is a test in this repository that CI runs.

- **Conformance to the protocol.**
  - Every request is written field by field from Apache Kafka's message schemas, and so is every
    response read.
  - Every encoding is checked against [`kafka-protocol`](https://crates.io/crates/kafka-protocol),
    a codec generated from those same schemas. It is a dev-dependency only, used as an oracle, at
    each version spoken.
  - Record batches v2 are checked in every codec: CRC-32C, control batches, aborted transactions
    skipped the way Kafka's `CompletedFetch` skips them, and LZ4 frames of the kind Kafka's
    reader accepts.
  - SCRAM reproduces RFC 7677's exchange byte for byte.
- **Real brokers** (`tests/brokers.rs`, CI's `brokers` job):
  - Redpanda 24.2;
  - Apache Kafka 4.1 (KRaft);
  - Redpanda 26.2 requiring SASL/SCRAM;
  - interoperability with `rpk` (franz-go, an independent implementation) both ways in every codec;
  - a lost answer replayed against each broker's idempotence state.
- **Exhaustive model checking of the producer** (`tests/produce_model.rs`).
  - Setup: the producer's rules, with a broker that keeps Kafka's idempotence state.
  - What is explored: every reachable state, covering every interleaving of sends, deadlines,
    batches of any size, lost requests and answers, retriable refusals, and duplicate answers.
  - Checked in every state:
    - no message is written twice or out of order;
    - every message gets exactly one outcome;
    - a reported offset is the one written;
    - a message reported "not persisted" is not in the log;
    - no request goes out past its message's deadline;
    - no request finds a sequence gap.
  - Bugs planted in the model are caught.
- **Exhaustive checking of read_committed.** The real reader runs on every log of up to 8 batches
  from two transactional producers and one non-transactional producer, from every offset, and is
  compared against Kafka's rule.
- **Kani proofs** (`src/proofs.rs`, CI's `kani` job). These hold for *every* input up to their
  bounds, not a sample:
  - varints round-trip for every `i64`;
  - no bytes make the varint, cursor or batch-header decoders panic or read out of bounds;
  - an array length from the wire is never trusted beyond the bytes that are actually present;
  - version negotiation picks the newest common version.
- **Fuzzing** (`fuzz/`, cargo-fuzz). Every response decoder and the record reader get arbitrary
  bytes. Nothing panics, and nothing is allocated beyond what the bytes allow. Decompression is
  capped, so a 1 MB zstd bomb is refused instead of growing into gigabytes.
- **Unit tests against an in-process broker** that drops connections, stalls, moves leaders,
  refuses, throttles and corrupts. A corrupt batch is fetched again on a new connection and
  never delivered.
- **The stress harness** (`stress/`).
  - The producer and consumer run against a real Redpanda through a proxy that, at random:
    - cuts connections mid-frame;
    - stalls them for seconds;
    - delays them;
    - refuses new ones.
  - Every outcome is checked against the broker's log:
    - nothing is written twice;
    - every report is true;
    - a key's order is kept;
    - the consumer reads every partition exactly as a reader without faults does.
  - It runs for 90 s on every PR and for 15 minutes nightly, with a new seed each run.
- **The benchmark gate** (`benches/gate.rs`, `bench/gate/gate.py`).
  - What it measures: the hot paths (reading records in every codec, writing batches, decoding
    a Fetch).
  - How: counted in instructions under Valgrind, which is deterministic to well below 0.1%.
  - When: on every PR, against the base branch built on the same runner.
  - A regression above 3% fails the PR.
- **Adversarial review.** Before release, independent reviewers went through the producer, the
  consumer, and the network/security layers looking for data loss, panics on broker input, and
  denial of service. Each finding they confirmed is a regression test now.

## Benchmarks

<!-- BENCH-TABLE -->

## Settings

Settings use librdkafka's names and defaults, except where noted. Unknown keys are refused.

- **Network:**
  - `bootstrap.servers`, `client.id`, `security.protocol`;
  - `sasl.mechanisms`, `sasl.username`, `sasl.password`;
  - `ssl.ca.location`, `ssl.ca.pem`, `ssl.certificate.location`, `ssl.certificate.pem`,
    `ssl.key.location`, `ssl.key.pem`;
  - `ssl.endpoint.identification.algorithm`, `enable.ssl.certificate.verification`;
  - `socket.timeout.ms`, `socket.connection.setup.timeout.ms`;
  - `reconnect.backoff.ms`, `reconnect.backoff.max.ms`;
  - `receive.message.max.bytes`.
- **Consumer:**
  - `auto.offset.reset`, `enable.partition.eof`, `isolation.level` (default `read_committed`);
  - `fetch.wait.max.ms`, `fetch.min.bytes`, `fetch.max.bytes`, `max.partition.fetch.bytes`;
  - `queued.min.messages`, `queued.max.messages.kbytes`;
  - `check.crcs` (default `true`);
  - `group.id` (for `Client::commit`).
  - `enable.auto.commit=true` is refused: fastka commits only when you ask it to.
- **Producer:**
  - `linger.ms` (default 5), `batch.size`, `batch.num.messages`;
  - `queue.buffering.max.messages`, `queue.buffering.max.kbytes`;
  - `message.timeout.ms`, `request.timeout.ms`;
  - `retry.backoff.ms`, `retry.backoff.max.ms`;
  - `compression.type` (`none`, `gzip`, `snappy`, `lz4`, `zstd`), `compression.level`;
  - `message.max.bytes`, `topic.metadata.refresh.interval.ms`.
  - `enable.idempotence=false` and any `acks` other than `all` are refused: the producer is always
    idempotent.

## License

MIT or Apache-2.0, at your option.
