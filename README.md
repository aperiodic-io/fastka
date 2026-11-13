# fastka

A Kafka client for Rust that is synchronous and pure Rust: no librdkafka, no OpenSSL, no async
runtime. The consumer fetches on your thread, and the producer is idempotent.

```toml
[dependencies]
fastka = "0.1"
```

fastka was written for one job: a market-data pipeline engine whose data thread used to spend
more time inside librdkafka than decoding the messages. It came out of that engine, where it
replaced librdkafka, as a crate that knows nothing of it. That origin explains both
what it is good at and the things it deliberately leaves out.

**At a glance** (1M messages of 100 B through Redpanda on the same 4-vCPU host, medians of 5
runs; [details below](#benchmarks)):

- **Consuming**, fastka spends the least CPU per message of the clients measured. That is
  0.24 µs, against 1.56 µs for librdkafka (via rdkafka) and 0.30 µs for rskafka. It reads about
  4M msgs/s on one thread.
- **Producing**, fastka has about 2× librdkafka's throughput at less than half its CPU per message.
- **Latency** from produce to consume is level with librdkafka (p50 0.34 ms against 0.37 ms).

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
- **Small.** About 4.6k lines of library code (comments aside), with 10 dependencies: rustls,
  ring, mio (on Unix), the codecs and crc32c. zstd is the only C code it builds.

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
| **Epoch bump on a sequence gap** | Where librdkafka bumps the producer epoch and renumbers, a fastka producer becomes *fatal* (`fatal_error()`), and you create a new one. This only happens when a batch may have been written but its outcome is unknown: its answer lost, or a refusal with a code not known to come before the broker appends. |

**Platforms.** CI runs on Linux. The consumer waits on socket readiness through mio's epoll or
kqueue on Unix. Elsewhere, Windows included, it compiles, but the consumer checks its sockets every
millisecond instead, and nothing there is tested.

## Production readiness

**Verdict: ready for production within the scope above.** That scope is assigned partitions; an
idempotent producer; PLAINTEXT, TLS or SASL PLAIN/SCRAM; Kafka 2.4 to 4.1 or Redpanda; Linux.
Each criterion below holds on the current commit, and CI checks it on every push:

| Criterion | Evidence |
|---|---|
| No data loss, duplication or reordering under faults | the exhaustive producer model; the stress harness through cut, stalled and refused connections, every outcome checked against the broker's log |
| Every delivery report is true, including "possibly persisted" | the model's invariants; the harsh stress profile (22 fatal producers, 33k possibly-persisted reports, all true) |
| No panic, hang or unbounded memory on any broker input | Kani proofs; fuzzing; capped decompression; byte-bounded queues; every timeout bounded (TLS included) |
| Interoperates with real brokers and other clients | Redpanda, Apache Kafka 4.1, SASL/SCRAM, franz-go both ways in every codec |
| Fails loudly, not silently | unknown or unsupported settings refused at start; unsupported broker versions refused at connect |
| Performance does not regress | the instruction-count gate, base against head, on the same runner |
| Defects found by people trying to break it are fixed | three rounds of adversarial review; each finding is a test that failed first |
| A public API that can evolve | builders and `#[non_exhaustive]` types; protocol internals hidden; every public item documented; MSRV 1.87 checked |

**What this verdict does not claim.** fastka hasn't yet carried other people's production traffic
for years, as librdkafka has, and its API is 0.x, so a 0.2 may break it. Run it behind the same
canary you would give any new dependency on a critical path.

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
  - version negotiation picks the newest common version;
  - every key lands on a partition the topic has, whatever its partition count.
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
  - Each profile (lz4, none, zstd, and a harsh one whose stalls outlast `message.timeout.ms`)
    runs for 90 s on every PR and for 15 minutes nightly, with a new seed each run. The last
    local run sent 31M messages; 22 producers went fatal in the harsh profile and 33k reports
    said "possibly persisted", and every invariant held.
- **The benchmark gate** (`benches/gate.rs`, `bench/gate/gate.py`).
  - What it measures: the hot paths (reading records in every codec, writing batches, decoding
    a Fetch).
  - How: counted in instructions under Valgrind, which is deterministic to well below 0.1%.
  - When: on every PR, against the base branch built on the same runner.
  - A regression above 3% fails the PR.
- **Adversarial review.** Before release, the code went through two rounds of review aimed at
  breaking it. Reviewers looked at the producer, the consumer and the network and security
  layers, hunting for data loss, panics on broker input, and denial of service.
  - The first round found about 40 defects, among them:
    - a Produce request with no size cap;
    - records skipped after a partition error;
    - a zstd decompression bomb;
    - a TLS read that could outlive its timeout;
    - a SASL password visible in `Debug` output.
  - The second round reviewed the fixes. It found 7 more, among them a consumer queue with no
    byte bound and a reconnect that overran `message.timeout.ms`.
  - Each finding was proved by a test that failed first, and that test now guards the fix.

## Benchmarks

Reproduce these with `scripts/brokers.sh && bench/compare/run.sh > out.jsonl && bench/compare/table.py out.jsonl`.
The raw runs are in [`bench/results/compare.jsonl`](bench/results/compare.jsonl).

**Setup:**
- 4 vCPUs and 15 GB of RAM; Linux 6.18.
- Redpanda 24.2 (`--smp 1`) in Docker on the same host.
- A topic of 3 partitions; 1,000,000 messages of 100 B.
- Each run is a fresh process. A produce run writes a fresh topic. A consume run reads a backlog
  written once, in batches of up to 10,000 messages, from the beginning of every partition.
- Wall time runs from creating the client to the last message delivered or read, so connection
  setup is included.
- CPU time is user plus system time of the whole process, *every thread included*
  (`getrusage`). This is what a client really costs a host, not only the caller's thread.

**The clients, and how each is driven:**
- **fastka**: `Producer::send` and `poll`; `Consumer::next_event`.
- **rdkafka 0.39** (librdkafka 2.12):
  - producing: `BaseProducer`, idempotent, `linger.ms=5`, the same batch sizes as fastka;
  - consuming: `BaseConsumer::poll` with assigned partitions, as librdkafka's per-message API
    is used.
- **rskafka 0.6** (async, tokio multi-thread runtime). It has no batching producer at this level,
  so the harness batches for it (10,000 messages or ~1 MB, one batch per partition in flight). It
  consumes `fetch_records` with the 3 partitions as 3 tasks, which run on 3 cores at once.
- **kafka-rust 0.10** (`kafka`): synchronous `send_all` of the same batches, `acks=all`,
  not idempotent. It has no lz4.

All clients write with `acks=all`.

**Consume, 1,000,000 messages of 100 B, compression: none**

| client | msgs/s | CPU µs / msg | peak RSS | runs |
|---|---:|---:|---:|---:|
| **fastka** | 4,089,980 | 0.24 | 24 MB | 5 |
| rdkafka | 276,365 | 1.56 | 57 MB | 5 |
| rskafka | 5,437,738 | 0.30 | 35 MB | 5 |
| kafka-rust | cannot read record batches v2 (Kafka ≥ 0.11's format): `UnexpectedEof` | | | |

**Consume, 1,000,000 messages of 100 B, compression: lz4**

| client | msgs/s | CPU µs / msg | peak RSS | runs |
|---|---:|---:|---:|---:|
| **fastka** | 3,608,805 | 0.26 | 79 MB | 5 |
| rdkafka | 295,438 | 1.25 | 120 MB | 5 |
| rskafka | 4,415,011 | 0.51 | 120 MB | 5 |

**Produce, 1,000,000 messages of 100 B, compression: none**

| client | msgs/s | CPU µs / msg | peak RSS | runs |
|---|---:|---:|---:|---:|
| **fastka** | 1,622,060 | 0.64 | 25 MB | 5 |
| rdkafka | 768,049 | 1.45 | 37 MB | 5 |
| rskafka | 1,787,630 | 0.77 | 22 MB | 5 |
| kafka-rust | 600,528 | 0.96 | 7 MB | 5 |

**Produce, 1,000,000 messages of 100 B, compression: lz4**

| client | msgs/s | CPU µs / msg | peak RSS | runs |
|---|---:|---:|---:|---:|
| **fastka** | 1,514,005 | 0.66 | 14 MB | 5 |
| rdkafka | 793,021 | 1.39 | 36 MB | 5 |
| rskafka | 477,168 | 0.67 | 17 MB | 5 |

**Produce → consume latency, 20,000 messages at 5,000/s, one partition (fastka's producer for both; the consumer differs)**

| consumer | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|
| **fastka** | 343 µs | 3,178 µs | 4,961 µs | 6,376 µs |
| rdkafka | 367 µs | 3,511 µs | 5,249 µs | 7,526 µs |

**How to read these numbers:**
- **CPU per message is the number fastka was built for.** Its consumer does all its work on the
  thread that calls it, with no per-message allocation behind a lock and no hand-off between
  threads. That is the gap to librdkafka, whose per-message API pays for a heap object per
  message, op queues and a thread hand-off.
- **rskafka reads faster in wall time** because it decodes three partitions on three cores,
  while fastka's consumer is one thread. Run one fastka consumer per thread over a share of the
  partitions to use more cores. rskafka's *producer* numbers depend on the harness's batching
  (above), so read them as an upper bound.
- **librdkafka's wall time** includes its threads' start-up and its fetch queue's hand-off. A
  long-running consumer amortizes the start-up but not the per-message CPU.
- **Latency:** in both latency runs fastka's producer writes (with `linger.ms=0`), and the two
  consumers read the same stream with `fetch.wait.max.ms=100`. So the table compares consumers.
  fastka's consumer blocks on its sockets' readiness (epoll/kqueue) between Fetches. It does not
  spin, so an idle consumer costs nothing.
- **This is one host with the broker local.** Over a real network, round trips dominate latency
  and all clients look closer.

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
  - `queued.min.messages`, `queued.max.messages.kbytes` (each a bound on a partition's queue);
  - `fetch.error.backoff.ms`;
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
