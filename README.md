<div align="center">

```
╔═══════════════════════════════════════════════════════════════╗
║  V I G I L   ·   R E A L - T I M E   A N O M A L Y   G R I D  ║
║       go · kafka · windowed state · aws lambda · terraform    ║
╚═══════════════════════════════════════════════════════════════╝
```

**A streaming pipeline that watches money, video and air — and tells you the moment one of them goes wrong.**

[![Go](https://img.shields.io/badge/Go-0a0e27?style=for-the-badge&logo=go&logoColor=00ffff)](https://go.dev)
[![Kafka](https://img.shields.io/badge/Kafka-0a0e27?style=for-the-badge&logo=apachekafka&logoColor=ff00ff)](https://kafka.apache.org)
[![Redpanda](https://img.shields.io/badge/Redpanda-0a0e27?style=for-the-badge&logo=redpanda&logoColor=9d00ff)](https://redpanda.com)
[![AWS Lambda](https://img.shields.io/badge/Lambda-0a0e27?style=for-the-badge&logo=awslambda&logoColor=00a8ff)](https://aws.amazon.com/lambda/)
[![Terraform](https://img.shields.io/badge/Terraform-0a0e27?style=for-the-badge&logo=terraform&logoColor=ffee00)](https://terraform.io)

</div>

---

Transactions, transcode jobs and air-quality sensors flow into Kafka. A stateful
processor keeps a time window per entity, runs five detection rules over it, and
publishes what it finds. A dashboard streams the result to your browser as it
happens, and gives you buttons to break things on purpose.

```
docker compose up
```

Then open **http://localhost:8080**, press **INJECT FAULT**, and watch a card get
used in São Paulo and Tokyo three seconds apart.

## What it detects

Five generic primitives over one event type, which is what lets a single
pipeline serve three unrelated domains instead of three pipelines serving one
each. Each stream enables the subset that means something for it.

| Rule | What it measures | payments | video | sensors |
|---|---|---|---|---|
| `velocity` | events per key inside the window | **card testing** | retry storm | flapping |
| `geovelocity` | implied speed between consecutive events | **impossible travel** | — | impossible relocation |
| `zscore` | deviation from *this key's* own recent history | amount anomaly | — | reading spike |
| `stall` | nothing arrived at all | — | **job stalled** | sensor offline |
| `threshold` | value held past a bound | — | SLA breach | air quality |

Every one of those is wired to a button on the dashboard, and every one has a
test asserting that injecting it raises that rule and not a different one.

## Measured, on a laptop

Against a single-broker Redpanda in Docker, three services, ordinary traffic:

| | |
|---|---|
| detection latency (engine) | **p50 13ms · p99 17ms** |
| end-to-end (produce → detect → consume, both Kafka hops) | **p50 25ms · p99 28ms** |
| false positives over 24 × 10-minute simulated runs | **0** |

The second number is the honest one — it includes both trips through the broker
and is what a person watching the dashboard actually waits for. Reproduce it
with `task up` then `task stats`.

## Architecture

```
  ┌───────────┐   payments · video · sensors        ┌──────────────────┐
  │ generator │──────────────────────────────┐      │    dashboard     │
  │    (Go)   │   fault injection API        │      │  SSE, no polling │
  └───────────┘                              │      └────────▲─────────┘
                                             ▼               │
                                    ╔════════════════╗       │
                                    ║  vigil.events  ║       │
                                    ╚════════╤═══════╝       │
                                             │               │
  ┌──────────────────────────────────────────▼────────────┐  │
  │ processor (Go)                                        │  │
  │   consumer group → window per key → five rules        │  │
  │   ├─ vigil.alerts ─────────────────────────────────┐  │  │
  │   ├─ vigil.state    compacted changelog, restored  │  │  │
  │   │                 on rebalance before consuming  │  │  │
  │   └─ vigil.dlq      records that cannot be decoded │  │  │
  └───────────────────────────────────────────────────┼───┘  │
                                                      │      │
                          ┌───────────────────────────┴──────┴───┐
                          │ gateway (Go) — fan-out, control API  │
                          └──────────────────────────────────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │   AWS Lambda     │  enrich → DynamoDB
                                    │  (LocalStack)    │  Terraform, unapplied
                                    └──────────────────┘
```

### Why the state store exists

When a partition moves to another consumer, the new owner needs the window
history for every key on it. Replaying the input topic to rebuild that is
unbounded work. So the processor writes a **compacted changelog** keyed by
entity — one record per live key — and replays *that* on assignment instead.

The ordering is the part that matters: the changelog is flushed **before**
offsets are committed, never after. A consumer that committed first would
acknowledge work whose state had not been made durable, and every rebalance
would silently lose a window's worth of history.

### Why not Kafka Streams

Kafka Streams solves this and solves it well. It is also **JVM-only**, and this
pipeline is Go — so the pieces it would have provided are implemented here:
partition-local state, sliding windows advanced by event time, a grace period
for out-of-order arrivals, and changelog-backed recovery.

**This is not a reimplementation of Kafka Streams and does not claim parity.**
[ADR 0001](docs/decisions/0001-go-core-java-topology.md) sets out exactly which
guarantees are reproduced and which are not — no interactive queries, no
exactly-once transactions across topics, no watermark propagation through a
topology. A `streams/` service runs a real Kafka Streams topology for the
rolling metrics, so the comparison in that document is between two things in
the same repository rather than between code and a memory.

## Running it

```bash
docker compose up          # broker + three services + dashboard on :8080
task up                    # the same thing, plus a friendlier message
task up:ui                 # ...and Redpanda Console on :8090
```

Then:

```bash
task inject -- payments impossible-travel
task surge                 # 2,000 events/sec for twenty seconds
task stats
task scale                 # a second processor — kill one, watch the rebalance
task dlq                   # read the dead-letter topic
```

From source, without containers for the services:

```bash
task broker                        # just Redpanda
KAFKA_BROKERS=localhost:19092 go run ./cmd/generator &
KAFKA_BROKERS=localhost:19092 go run ./cmd/processor &
KAFKA_BROKERS=localhost:19092 go run ./cmd/gateway
```

Go 1.25+. Nothing else is required, and there are no credentials to configure.

## Five topics, and why that number is load-bearing

| Topic | Partitions | Carries |
|---|---|---|
| `vigil.events` | 2 | all three streams, keyed by entity |
| `vigil.alerts` | 2 | rule hits |
| `vigil.metrics` | 2 | rolling aggregates from the Kafka Streams topology |
| `vigil.state` | 2 | compacted changelog of the window store |
| `vigil.dlq` | 1 | records that could not be processed |

Aiven's free Kafka plan — the only managed Kafka that is free indefinitely, with
no card and no expiry — allows **five topics of two partitions**. That ceiling is
why all three streams share one input topic rather than getting one apiece. It
is a design constraint, not an oversight: a sixth topic changes the hosting
story.

**The Kafka Streams service does not fit inside it.** Re-keying events from
entity to stream name forces a shuffle, so Streams provisions a repartition
topic and a changelog for its state store — seven topics in total, and it
manages them itself:

```
vigil-streams-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition
vigil-streams-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog
```

That is the honest shape of the trade. The Go processor's changelog is one
topic that was chosen and named deliberately; Kafka Streams created two without
being asked, which is exactly the convenience it exists to provide and exactly
what makes it not free. A free-tier deployment runs the Go core; a self-hosted
one runs everything. `docker compose up` has no such limit and runs all of it.

Two partitions is also enough for the demonstration worth making. Run
`task scale`, stop one processor, and watch the survivor take both partitions
and rebuild their state from the changelog before it resumes.

## Configuration

Every variable is optional; the defaults point at the local broker.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:19092` | comma-separated seed brokers |
| `KAFKA_USER` / `KAFKA_PASSWORD` | — | SASL/SCRAM-SHA-512, as Aiven issues. Setting a user implies TLS |
| `KAFKA_TLS` | `false` | force TLS without credentials |
| `KAFKA_GROUP` | `vigil-processor` | consumer group |
| `GATEWAY_ADDR` · `GEN_ADDR` · `PROC_ADDR` | `:8080` `:8081` `:8082` | listen addresses |
| `GENERATOR_URL` | `http://localhost:8081` | where the gateway forwards controls |
| `LOG_LEVEL` | `info` | `debug` for per-record detail |

## Layout

```
cmd/
  generator/   three simulators, rate control, fault injection
  processor/   consumer group → rules → alerts, dead letters, changelog
  gateway/     SSE fan-out, control API, and the embedded dashboard
internal/
  domain/      Event · Alert · Geo            ← pure, no I/O
  rules/       the five detection primitives  ← pure
  window/      sliding windows on event time  ← pure
  state/       the window store and its snapshot format
  engine/      rules + store + cooldown, and no idea Kafka exists
  sim/         the traffic, ordinary and broken
  kafkax/      topics, client, codec, dead letters
  stream/      the consume loop and changelog restore
  httpapi/     JSON, SSE broker, graceful shutdown
  telemetry/   latency percentiles and rates
streams/       Kafka Streams topology (Java 21) — rolling metrics
lambda/        Go handler for the event-driven tier
infra/         Terraform (unapplied), and deploy configs
```

The five packages above the Kafka line are at **100% statement coverage** and
`task check` enforces it. That is not a vanity number: everything in them is
decidable without a broker, so anything uncovered there is untested by choice.

## Tests

```bash
task check     # gofmt · go vet · go test -race · coverage gate
task test      # just the tests
```

Two of them carry more weight than the rest:

- **Every fault trips the rule it is named for.** Nine faults, each injected
  into a real engine over simulated time, each asserted to raise its own rule.
- **Ordinary traffic raises nothing.** Twenty-four independent ten-minute runs
  across three streams and eight seeds. A detector that alerts on normal
  behaviour is worse than no detector, and this is the property that keeps the
  thresholds in `rules.DefaultProfiles` honest — they were retuned three times
  to satisfy it.

Four bugs were found by running the thing rather than by reading it, and each
left a regression test behind. The most interesting: fault events were stamped
with `time.Now().Add(d)`, which keeps the monotonic reading — so their release
timing was correct — but serialises a **wall-clock** value computed as though
the wall clock advances at the monotonic rate. Under NTP slew it does not. The
result was events arriving stamped in the future, and alerts detected before
the events that caused them. See `TestScheduledEventsAreStampedAtRelease`.

## Licence

MIT. See [LICENSE](LICENSE).

---

<div align="center">

Built by **Luis Felipe Ribeiro Vieira** ·
[portfolio](https://luisfelipe-theta.vercel.app) ·
[GitHub](https://github.com/Lauiskk) ·
[LinkedIn](https://www.linkedin.com/in/luisinfelipe)

</div>
