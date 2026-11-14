# kafka-stress

fastka's producer against librdkafka's (rdkafka 0.39), under the shape of a metrics pipeline at the
top of the hour: pipelines sharing a producer, a steady trickle, and every window closing at once.
It was written (in brrrrr, before fastka was extracted from it) to check a staging report: fewer,
bigger Produce requests at the top of the hour, and leader step-downs.

The clients reach the brokers through a proxy in the harness. The proxy records every Produce
request: its size, its partitions, how many requests are outstanding on its connection, and the
broker's latency. It can delay answers (`--delay-ms`, a cluster's replication) or hold them back
(`--stall`). Afterwards every partition is read back, and each delivered message must be there
exactly once, in its pipeline's order. If not, the exit code is 1.

It is its own workspace, outside fastka's package (`bench/` is excluded): it builds librdkafka,
which fastka never links. rdkafka is built without default features (`libz` and `zstd` only), so
no curl or SASL C libraries are needed, as in `bench/compare`.

```bash
cargo build --release
./cluster.sh                      # 3-node Redpanda, CPUS=0.6 each; or one node for run-matrix.sh
./cluster-matrix.sh --producers 1 --duration 30 --period 12 --first 4 --size 500 --burst 30000
```

Each matrix runs the client three ways:
- fastka with its defaults;
- fastka with `max.in.flight=5 max.request.size=1048576` (librdkafka's request shape);
- librdkafka.

`kafka-stress --help` lists every flag. `-F key=value` sets a fastka setting, `-R key=value` a
librdkafka one, and `-X key=value` one for both.

## Note on the numbers below

**They were measured on brrrrr-kafka**, the crate fastka was extracted from, with the change of
aperiodic-io/brrrrr#355 (`max.in.flight`, `max.request.size`, batches encoded off the lock),
**before that change was ported to fastka**. They have not been re-run on fastka. "brrrrr-kafka"
in the tables is the ancestor of what this harness now runs as `--client fastka`; "+ knobs" is
`-F max.in.flight=5 -F max.request.size=1048576`.

One difference matters when reading them: brrrrr-kafka's requests had no bound by default, so the
first rows show requests of up to 10 MB. fastka bounds a request at `max.request.size`, which
defaults to `message.max.bytes` (1000000). So fastka with its defaults already sends requests of
at most about 1 MB, but with one request outstanding per broker.

## Results (2026-11-13, Redpanda v24.2.7, one producer, 16 pipelines, 24 partitions)

A burst is a window of 3 s from each top of the hour.

| scenario | client | burst req/s | KB/req avg | KB/req p99 | parts/req max | delivery p99 | read back | elections |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 node, answers at 20 ms | brrrrr-kafka | 46 | 476 | 7,600 | 12 | 435 ms | exactly once | - |
| | + knobs | 78 | 281 | 949 | 1 | 424 ms | exactly once | - |
| | librdkafka | 75 | 294 | 976 | 1 | 403 ms | exactly once | - |
| 1 node, answers at 300 ms | brrrrr-kafka | 3.3 | 6,476 | 9,645 | ~10 avg | 1.5 s | exactly once | - |
| | + knobs | 16.7 | 804 | 954 | 1 | 2.3 s | exactly once | - |
| | librdkafka | 16.7 | 667 | 976 | 1 | 5.0 s, 95k timed out | **10,889 dups, 158 out of order** | - |
| 3 nodes RF 3, 0.3 CPU, 78 MB/s | brrrrr-kafka | 46 | 1,729 | 10,614 | - | 1.9 s | exactly once | 0 |
| | + knobs | 110 | 730 | 966 | 1 | 1.07 s | exactly once | 0 |
| | librdkafka | 135 | 595 | 976 | 1 | 1.6 s | exactly once | 0 |
| 3 nodes RF 3, 0.15 CPU (2 runs) | brrrrr-kafka | 22-26 | 2,400 | 7,900 | - | 2.8-3.1 s | exactly once | 0, 0 |
| | + knobs | 56-68 | 840 | 966 | 1 | 2.7-2.8 s | exactly once | 0, 0 |
| | librdkafka | 76-95 | 700 | 976 | 1 | 2.1-2.9 s | exactly once | 4, 3 |

Broker stalls (3 nodes, every answer held back from 6 s):

| stall | brrrrr-kafka (either config) | librdkafka |
| --- | --- | --- |
| 3 s | no failures, exactly once | no failures, exactly once |
| 6 s, past `message.timeout.ms` 4.5 s | fatal: 82-93k of 96k fail; latest delivery 3.8 s | 1,136 fail; **a delivery at 5.8 s, past its timeout** |

What this shows:

- **The staging pattern is reproduced, and it comes from the client.** librdkafka sends one
  partition per Produce request, at most 1 MB, with up to 5 outstanding per connection.
  brrrrr-kafka kept one request outstanding per broker, and everything that comes due meanwhile
  goes into the next one, so requests grow with the broker's latency: up to 12 partitions and
  10 MB here.
- **The step-downs are not reproduced.** No brrrrr-kafka run, in either config, caused a leader
  election on a CPU-starved 3-node cluster. librdkafka's runs caused 3-4. The cause on staging
  still isn't shown; the A/B is the test that decides it.
- **The knobs give librdkafka's request shape.** Delivery is as fast or faster, except when the
  broker is slow for every request: 5 x 1 MB per round trip then caps throughput (300 ms row).
- **Exactly once holds** in every brrrrr-kafka run. librdkafka duplicated and reordered under
  timeouts (the epoch bump), and delivered past its timeout, which the lease fence rests on.

## The send path, before and after encoding moved off the lock

The tables above were measured before this change. At that point, each batch was encoded while
the producer's lock was held, so every `send()` and every other broker thread waited for each
batch's compression and CRC. A burst now enqueues and delivers sooner. These runs used one broker,
1 producer, 16 pipelines, a 64 MB burst and `queue.buffering.max.messages=2000000`, so the queue
never fills. "Enqueue" is how long a pipeline takes to hand over its burst.

| broker | client | delivery p50 | delivery p99 | enqueue p50 |
| --- | --- | --- | --- | --- |
| 2 CPUs (3 runs) | brrrrr-kafka, before | 190-236 ms | 372-404 ms | 363-457 ms |
| | brrrrr-kafka, after | 66-174 ms | 137-244 ms | 258-302 ms |
| | + knobs, after (2 runs) | 46-97 ms | 167-204 ms | 252-278 ms |
| | librdkafka (2 runs) | 171-202 ms | 265-311 ms | 298-325 ms |
| 0.4 CPU (2 runs) | brrrrr-kafka, after | 153-154 ms | 267-275 ms | 238-278 ms |
| | + knobs, after | 109-110 ms | 215-242 ms | 317-320 ms |
| | librdkafka | 172-216 ms | 260-320 ms | 317-332 ms |

Every run read back exactly once, in order. `kafka-stress` prints the send path's own numbers
(`send path:`): each burst's enqueue time and each `send()` call's.

## max.in.flight pipelined on one connection

At first `max.in.flight` gave each broker N threads and N connections (brrrrr #355's review).
It is now librdkafka's: one thread and one connection per broker, with up to N requests
pipelined on it. Measured on brrrrr-kafka before the move, same scenario as above, at 2 CPUs,
2 runs:

| client | delivery p50 | delivery p99 | requests outstanding on a connection, at most |
| --- | --- | --- | --- |
| pinned (`max.in.flight=1`, unbounded requests) | 104-117 ms | 207-239 ms | 1 |
| `max.in.flight=5`, `max.request.size=1048576`, pipelined | 94-150 ms | 233-277 ms | 3 |
| librdkafka | 171-190 ms | 270-276 ms | 5 |

With the knobs, p99 is level with librdkafka. It was 167-204 ms with N connections, because a
broker works through one connection's requests in turn.

fastka with the same knobs (one producer, 16 pipelines, 24 partitions; `-F max.in.flight=5
-F max.request.size=1048576`):

| scenario | requests outstanding, at most | delivery p99 | failed | read back |
| --- | --- | --- | --- | --- |
| answers at 300 ms | 4 | 2.29 s | 0 | exactly once, in order |
| a 3 s stall | 5 | 2.83 s | 0 | exactly once, in order |
| a 6 s stall (past `message.timeout.ms` 4.5 s) | 5 | - | 89,760 (fatal, as designed) | exactly once, in order |

Not modelled: an application's recovery from a fatal producer (the harness counts its failures), many
topics, staging's broker settings.
