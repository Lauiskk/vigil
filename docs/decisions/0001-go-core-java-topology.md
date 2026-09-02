# 0001 · A Go core, and one Kafka Streams topology beside it

**Status:** accepted · **Date:** 2026-09-02

## Context

The brief was a real-time pipeline using Kafka for ingestion, **Kafka Streams**
for processing, and AWS Lambda for event-driven functions.

Kafka Streams is a Java library. There is no port, and the ecosystem has not
produced an equivalent outside the JVM — a claim worth substantiating rather
than asserting, so the survey is at the bottom of this document.

The pipeline itself had to be Go. That is the language the work is in, and a
portfolio piece whose centrepiece is written in something else is arguing
against itself.

## Decision

**The pipeline is Go. One Kafka Streams topology sits beside it**, consuming
`vigil.events` and producing rolling aggregates to `vigil.metrics`.

The Go processor implements the parts of the Kafka Streams model it needs:

- partition-local state, keyed by entity
- sliding windows advanced by **event time**, not wall clock
- an explicit grace period for out-of-order arrivals
- a **compacted changelog** topic, replayed on partition assignment
- changelog durability ordered **before** offset commit

The Java service is not a fallback or a demonstration of the same thing twice.
It does a job the Go side deliberately does not: tumbling aggregation over a
`KTable`, which is what Kafka Streams is genuinely good at and what would have
been the most tedious part to hand-roll.

## What is reproduced, and what is not

Being precise about this is the entire value of the document. A README claiming
"Kafka Streams in Go" would be wrong, and a reviewer who knows Kafka Streams
would spot it in about a minute.

| Kafka Streams provides | Here | Notes |
|---|---|---|
| Partition-local state store | **yes** | `internal/state`, in memory, bounded per key |
| Changelog-backed recovery | **yes** | `vigil.state`, compacted; replayed on assignment |
| Event-time windowing | **yes** | `internal/window`, sliding, watermark per key |
| Grace period for late records | **yes** | configured per stream; late records are counted, not silently dropped |
| Standby replicas | no | recovery is a cold changelog replay; a rebalance costs seconds |
| Interactive queries | no | state is not addressable from outside the process |
| Exactly-once across topics | **no** | at-least-once, with idempotent producers. See below |
| Watermark propagation through a topology | no | there is no topology — one processing stage, one window per key |
| Windowed joins, `KTable` semantics | in the Java service only | which is why it is there |
| RocksDB spill to disk | no | bounded in-memory windows; a key over its cap degrades to its most recent observations |

### On exactly-once specifically

The Go processor is **at-least-once**. Producers are idempotent, so a retry
does not duplicate within a partition, and the changelog-before-commit ordering
means recovery replays rather than loses. But an alert published and then
reprocessed after a crash can be published twice.

That is a deliberate choice, not an omission. Kafka transactions across the
input topic, the alert topic and the changelog would give exactly-once, at the
cost of transactional overhead on every batch — and the consumer of these
alerts is a dashboard and a Lambda that writes to DynamoDB keyed by alert id,
both of which are idempotent at the point of use. Paying for exactly-once to
serve two consumers that do not need it is the wrong trade.

If the alert stream ever fed something that charges money, this decision would
have to be revisited, and the note above is where to start.

## Consequences

**Good.** One language for the pipeline, small static binaries, no JVM in the
critical path, and every rule testable without a broker — the five packages
above the Kafka line reach 100% coverage precisely because nothing in them
knows what Kafka is.

Writing the windowing by hand also surfaced two bugs that a library would have
hidden: comparing an arriving event against the *second-newest point in the
window* rather than against its own predecessor, which is only the same thing
while records arrive in order; and a z-score rule that alerted on ordinary
readings because sigma estimated from twenty-eight samples carries its own
thirteen-percent standard error. Both are in the test suite now.

**Bad.** Two toolchains, two build systems, two CI jobs. The Java service is
the largest container image in the compose file by a factor of twenty. And the
list above is a maintenance burden: every row is a promise that has to stay
true.

**Ugly.** "Why didn't you just use Kafka Streams?" is now a question with a
long answer instead of no answer. That is the intended outcome — but it is only
an improvement if the long answer is correct, which is why this file exists and
why it names what is missing before it names what is present.

## Alternatives considered

### Write the whole processor in Java with Kafka Streams

Literal compliance with the brief, and Kafka Streams is genuinely excellent at
this. Rejected because the centrepiece of the portfolio would then be Java
while the CV says Go and Elixir, and because defending someone else's library
in an interview is a shorter conversation than defending your own state store.

### `goka`

The closest thing to Kafka Streams in Go, and closer than it usually gets
credit for: processors with local state stores backed by compacted Kafka group
tables, which is real changelog-backed state rather than a cache.

Rejected on two grounds. It has **no windowing at all** — no tumbling, hopping
or session windows, no watermarks — so the part of Kafka Streams this pipeline
most needed is the part goka does not have. And its 2026 commit history is five
janitorial changes: Go version bumps, a `gofix` run, Docker Compose v2, podman
support. It is maintained, not developed.

### `segmentio/kafka-go`

Rejected as the client. Two hundred and sixty-five open issues and still on
v0.x after years. `franz-go` is pure Go, needs no cgo — which matters for
static binaries and ARM cross-compilation — supports Kafka 0.8 through 4.2, and
carries eleven open issues against three thousand stars.

### Redpanda Connect (formerly Benthos)

Has windowing and `group_by`, and is very actively developed. Rejected because
it is a **YAML-configured stream processor, not a library you write Go in**: as
a portfolio piece it would demonstrate operating someone else's engine rather
than understanding one. The licence also moved from MIT to BSL for much of it.

### Elixir Broadway

Excellent at what it does — concurrency, batching, backpressure, rate limiting,
graceful shutdown — and `broadway_kafka` is under active revival after a
two-year gap. But it is a **consumer pipeline framework, not a stateful stream
processor**: no local state stores, no changelog, no windowing. It solves the
half of the problem this project did not have.

## References

- `internal/window/sliding.go` — the windowing, and why retention and the query
  window are separate
- `internal/stream/processor.go` — the flush-then-commit ordering
- `internal/stream/restore.go` — changelog replay on partition assignment
- `streams/` — the Kafka Streams topology
