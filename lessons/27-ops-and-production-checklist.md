# Lesson 27 — Ops Toolbox & Production Checklist

> **Part 5 — Production** · 25 minutes

---

## What you'll learn

- The CLI commands worth memorising, and what each one tells you
- How to replay, skip, and rebalance in a real incident
- Why graceful shutdown is a correctness concern, not a tidiness one
- What separates this demo from a cluster you'd trust with money

---

## Why this matters

Twenty-six lessons of building. This one is about the day something breaks and you have a terminal, a paging alert, and no idea which of your assumptions is wrong.

Everything here is reachable from Kafka UI too. Learn the CLI anyway: it works over SSH into a locked-down bastion, it composes with `grep` and `jq`, and it doesn't lie to you about state by caching for thirty seconds.

---

## Before you start

[Lesson 26](26-observability.md). Cluster running.

A reminder from Lesson 03: inside a broker container, use the **internal** listener, or you'll get connection warnings for nodes 2 and 3.

```bash
--bootstrap-server kafka-1:29092
```

---

## The toolbox

### Is the cluster actually a cluster?

```bash
docker exec kafka-1 kafka-metadata-quorum \
  --bootstrap-server kafka-1:29092 describe --status
```

`LeaderId` is a real node, `CurrentVoters` lists three. If `LeaderId: -1`, the brokers can't see each other — a networking problem, not a Kafka one. Nothing else you try will work until this does.

### What shape is this topic, and is it healthy?

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic wikimedia-stream
```

Read `Isr` against `Replicas`. Equal means healthy. Shorter means a replica has fallen behind or died, and you're closer to `min.insync.replicas` rejecting writes (Lesson 06).

The fast version, for a cluster with many topics:

```bash
# Any partition whose ISR is smaller than its replica set
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --describe --under-replicated-partitions

# Partitions with no leader at all — writes and reads are both failing
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --describe --unavailable-partitions
```

Empty output from both is what you want. `--under-replicated-partitions` is the single best one-line health check for a Kafka cluster.

### Who is consuming, and how far behind?

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group wikimedia-consumer-group
```

The columns, and what each one means when it's wrong:

- **`LAG`** growing → consumers slower than producers
- **`CONSUMER-ID` is `-`** → nobody is assigned; the group has no members
- **one partition's lag growing, others flat** → a poison pill or a hot key (Lessons 04, 20)
- **`CURRENT-OFFSET` frozen** → the consumer is stuck, not slow

```bash
# Every group on the cluster
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --list
```

### How much data is in a partition?

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

```
wikimedia-stream:0:978
```

`topic:partition:end-offset`. Add `--time -2` for the *start* offset. `end − start` is how many records are currently retained — not `end`, which counts every record ever written (Lesson 03).

### What's actually in the dead-letter topic?

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream.dlt \
  --from-beginning --property print.headers=true
```

The numeric headers print as garbage — they're big-endian bytes, not strings (Lesson 22). For anything beyond "is it empty," read them from the DLT consumer's logs.

### Read one specific record

The DLT headers gave you `originalPartition` and `originalOffset`. Use them:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream \
  --partition 1 --offset 8423 --max-messages 1
```

This is the single most useful debugging command in Kafka, and almost nobody knows `--partition` and `--offset` exist.

### Change a topic config without a restart

```bash
docker exec kafka-1 kafka-configs \
  --bootstrap-server kafka-1:29092 \
  --alter --entity-type topics --entity-name wikimedia-stream \
  --add-config retention.ms=86400000
```

Recall from Lesson 09 that `NewTopic` beans **do not** update an existing topic's config. This command is how retention, `min.insync.replicas`, and compression actually get changed on a live topic. Read them back with `--describe`.

---

## Incident recipes

### Replay from a point in time

*"The bad deploy went out at 09:15. Reprocess everything since."*

Stop the consumer first — Kafka refuses to move offsets under a live group.

```bash
# 1. Dry run. No --execute means it only prints what it would do.
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group --topic wikimedia-stream \
  --reset-offsets --to-datetime 2026-07-10T09:15:00.000

# 2. Do it.
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group --topic wikimedia-stream \
  --reset-offsets --to-datetime 2026-07-10T09:15:00.000 --execute
```

Restart the consumer. It reprocesses from 09:15.

**This only works if your processing is idempotent.** Reprocessing six hours of records against a non-idempotent consumer double-applies six hours of side effects. That's what the `(partition, offset)` unique constraint from Lesson 18 was for — and note it does *not* protect a replay from the DLT, which produces new offsets (Lesson 22).

### Skip a poison pill

*"One record is blocking a partition and I need throughput back now."*

The blunt instrument:

```bash
--reset-offsets --to-latest --execute
```

This skips **every** pending record on every partition, not just the bad one. You've traded one lost record for all of them.

Better, if you know the offset:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group \
  --topic wikimedia-stream:1 \
  --reset-offsets --to-offset 8424 --execute
```

`topic:partition` targets one partition. Move past offset 8423 and nothing else.

Best: have a dead-letter topic, and never need this. That's Part 4.

### Rebalance leadership after a broker restart

Lesson 02's leaders-skew column goes bad after a restart: the recovered broker rejoins as a follower and never takes leadership back.

```bash
docker exec kafka-1 kafka-leader-election \
  --bootstrap-server kafka-1:29092 \
  --election-type preferred --all-topic-partitions
```

Leadership returns to the **preferred leader** — the first broker listed in each partition's `Replicas`. Skew returns to near zero.

### Add partitions (and understand what you just did)

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --alter --topic wikimedia-stream --partitions 6
```

Instant, irreversible, and on a **keyed** topic it re-maps `murmur2(key) % partitionCount` for most keys. Existing records stay where they are; new records for the same key may go elsewhere. Per-key ordering breaks across the boundary (Lesson 04).

There is no `--partitions 3` to undo it.

---

## Graceful shutdown

The producer buffers records in memory (Lesson 12). Kill the JVM and everything in the accumulator is gone — records your application believed it had sent.

`KafkaTemplate.flush()` blocks until every buffered record is acknowledged:

```java
package com.javaguy.producer.producer;

import jakarta.annotation.PreDestroy;

@Service
public class WikimediaProducer {

    // ... constructor, sendMessage ...

    @PreDestroy
    public void drainOnShutdown() {
        log.info("Flushing producer buffer before shutdown");
        kafkaTemplate.flush();
    }
}
```

Pair it with Spring Boot's graceful shutdown, so in-flight HTTP requests finish before the context closes:

```yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

On the consumer side, Spring stops the listener containers before closing the context, so an in-flight `@KafkaListener` invocation completes and its `acknowledge()` runs. That's why `SIGTERM` is safe and `kill -9` is not: the latter loses the offset commit for work you already did, and you reprocess on restart.

Kubernetes sends `SIGTERM`, waits `terminationGracePeriodSeconds`, then `SIGKILL`. Make sure that grace period exceeds your longest listener invocation.

---

## The production checklist

What separates this repository from a cluster you'd trust with money.

### Security — none of this project has any

- [ ] **TLS everywhere.** Replace the `PLAINTEXT` listeners with `SSL` or `SASL_SSL`. Every byte in this project — including your data — currently crosses the network in the clear.
- [ ] **Authentication.** SASL/SCRAM-SHA-512 or mTLS, for both client-to-broker and broker-to-broker. Right now any process that can reach port 9092 is a fully privileged client.
- [ ] **Authorization.** Kafka ACLs (`kafka-acls`) restricting which principals may produce to or consume from which topics. Without them, your consumer can delete your topics.
- [ ] **Secret management.** No passwords in `application.yml`. Environment variables at minimum; Kubernetes Secrets or Vault properly. This project has `password: password` committed to git.

### Cluster

- [ ] **Dedicated controller nodes.** Above ~5,000 partitions, split `KAFKA_PROCESS_ROLES` so controllers aren't competing with data I/O for disk and page cache (Lesson 07).
- [ ] **JVM heap around 6 GB.** Kafka's performance comes from the OS page cache, not the heap. A large heap steals memory from the cache and lengthens GC pauses. `-Xms6g -Xmx6g`.
- [ ] **Fast, dedicated disks.** Kafka is I/O bound. NVMe for `KAFKA_LOG_DIRS`, separate from the OS disk.
- [ ] **`auto.create.topics.enable=false`.** Otherwise a typo in a topic name creates a topic instead of failing (Lesson 08).
- [ ] **Pin every image tag.** `grafana/tempo:latest` is how Bug 3 in Lesson 26 happened.

### Topics

- [ ] **RF 3, `min.insync.replicas` 2, `acks=all`.** Any two of these without the third gives you a comfortable illusion (Lesson 06).
- [ ] **Partition count decided deliberately.** It only goes up, and raising it re-maps every key.
- [ ] **Retention sized by replay need, not disk.** `retention.bytes` is per partition, and it's an OR with `retention.ms` — whichever trips first wins (Lesson 09).
- [ ] **DLT retention longer than the source, with no size cap.** Failed records wait for a human (Lesson 21).

### Producers

- [ ] **`enable.idempotence=true`.** Default since Kafka 3.0. Set it explicitly so a conflicting config fails at startup instead of silently downgrading.
- [ ] **Handle the `CompletableFuture`.** A lone `log.error` in the callback is silent data loss (Lesson 13).
- [ ] **`linger.ms` > 0 if you enabled compression.** Otherwise you compress batches of one and conclude compression doesn't work (Lesson 12).
- [ ] **`flush()` on shutdown.**

### Consumers

- [ ] **`enable-auto-commit=false`, ack after the side effect.** Reversing those two lines converts a recoverable outage into permanent data loss (Lesson 18).
- [ ] **Idempotent processing.** At-least-once is not optional; duplicate handling is how you survive it.
- [ ] **A dead-letter topic**, with an error handler that classifies exceptions as retryable or not (Lessons 20–21).
- [ ] **Processing faster than `max.poll.interval.ms`.** Slow listeners cause rebalance storms and duplicate work, and no amount of `session.timeout.ms` helps (Lesson 19).
- [ ] **`CooperativeStickyAssignor`**, so a rolling restart doesn't stop the world for every member.

### Schema

- [ ] **Schema Registry with `BACKWARD` compatibility**, and `auto.register.schemas=false` in production so schemas are registered by CI, not by whichever producer started first (Lesson 25).
- [ ] **`ErrorHandlingDeserializer`** wrapping the Avro deserializer, or migrating to Avro silently disables your dead-letter path.

### Observability

- [x] **Metrics, traces, and logs** pushed via OTLP; `kafka-exporter` for lag.
- [ ] **Alert on sustained lag growth**, not absolute lag — a replay legitimately produces enormous lag.
- [ ] **Alert on consumer group member count = 0.** Lag cannot see this; a dead group has zero lag growth only because nothing is committing.
- [ ] **Alert on the DLT's produce rate.** Dead-lettering *reduces* source lag. A pipeline can discard every record with every dashboard green (Lesson 21).
- [ ] **Alert on under-replicated partitions.**
- [ ] **Sample traces below 100%**, ideally tail-based so failures are always kept.
- [ ] **Verify telemetry end-to-end.** All four bugs in Lesson 26 left the app healthy. "The container is up" is not evidence.

---

## Try it yourself

1. Run `--describe --under-replicated-partitions`, then `docker stop kafka-3`, and run it again. How long until the partitions appear? Which timeout governs that delay?

2. Restart a broker, check leader skew in Kafka UI, then run a preferred-leader election. Quantify the before and after.

3. Add `@PreDestroy` with `flush()` to the producer. Send 10,000 records and `Ctrl-C` mid-stream. Compare the topic's end offset with and without the flush.

4. Pick the three checklist items you'd do first for a service handling payments. Justify the order. (There's no single right answer, but "TLS before dedicated controller nodes" is defensible and the reverse is not.)

---

## Common mistakes

**`--reset-offsets` without `--execute`.**
A dry run. Read the `WARN`.

**`--reset-offsets` with the consumer running.**
Kafka refuses. Stop the group.

**`--to-latest` to skip one bad record.**
Skips every pending record on every partition.

**Adding partitions to a keyed topic without thinking.**
Instant, irreversible, and it re-maps your keys.

**`kill -9` on a consumer.**
Loses the offset commit for work already done. Reprocessed on restart.

**Treating the checklist as done because the demo works.**
This project has no TLS, no auth, no ACLs, and a password in git. It is a teaching artifact.

---

## Check your understanding

**1. `--under-replicated-partitions` returns empty, but writes are failing with `NotEnoughReplicasException`. How?**

<details>
<summary>Reveal answer</summary>

They can't both be true at the same instant — but they can be seconds apart, and that's the point.

`NotEnoughReplicasException` means the ISR was below `min.insync.replicas` **when the write arrived**. `--under-replicated-partitions` reports the ISR **now**. A replica that briefly fell behind — a GC pause, a slow disk flush, a network blip — leaves the ISR, rejects the writes that arrive during that window, catches up, and rejoins.

By the time you run the command, everything is healthy. The producer's error is real and the cluster's health is real.

This is why you alert on the *metric over time* (`kafka_under_replicated_partitions`) rather than running a command after the fact. Transient ISR shrinkage under load is one of the most common causes of intermittent produce failures, and it is invisible to point-in-time inspection.

</details>

**2. You reset a consumer group to `--to-datetime` six hours ago to replay a bad deploy. Your consumer writes to a database with a unique constraint on `(kafka_partition, kafka_offset)`. Is the replay safe?**

<details>
<summary>Reveal answer</summary>

Yes, for this replay — and it's worth being precise about why.

Resetting offsets makes the consumer re-read the **same physical records**: same partition, same offset. Each `save()` therefore presents the same `(partition, offset)` pair as the original write, hits the unique constraint, and is rejected as a duplicate. The replay is idempotent.

What is *not* safe is replaying from the **dead-letter topic**. That means producing the record's value back to the source topic, where it gets a **new offset** (and possibly a new partition). The constraint sees a fresh key and inserts a second row (Lesson 22).

So the same idempotency key protects one kind of replay and not the other. Replay-safety across republication requires a key derived from the event's content or a producer-assigned ID — Wikimedia's `meta.id`, an order ID, a request ID.

</details>

**3. Your team sets `min.insync.replicas=3` on an RF-3 topic "to be extra safe." What did they actually buy?**

<details>
<summary>Reveal answer</summary>

Zero fault tolerance for writes, and no additional durability.

With `acks=all` and a healthy ISR of 3, the write already reaches all three replicas. Raising the floor to 3 doesn't make it *more* durable — it just forbids the write when the ISR is anything less.

So now: any single broker restarting — a rolling deploy, a kernel patch, a spot-instance reclaim — drops the ISR to 2, which is below the floor, and every `acks=all` write is rejected until it rejoins and catches up. A routine deploy becomes a write outage.

`min.insync.replicas = RF − 1` is the largest floor that still tolerates one failure. With RF 3, that's 2 (Lesson 06).

</details>

**4. Why is `kill -9` on a consumer a correctness problem rather than an availability one?**

<details>
<summary>Reveal answer</summary>

Because it can destroy an offset commit for work that already happened.

The listener processes a record, writes to the database, and calls `acknowledge()`. With `MANUAL_IMMEDIATE` the commit is issued but is asynchronous — it's in flight. `SIGKILL` terminates the process before the broker records it.

On restart, the group resumes from the last *durably committed* offset, which is before that record. It's redelivered, reprocessed, and the database write happens twice. Without an idempotency key, you have double-applied a side effect.

`SIGTERM` gives Spring the chance to stop the listener containers, let in-flight invocations finish, and flush pending commits before the context closes. The offset then reflects the work actually done.

The same argument applies to the producer, in the other direction: `kill -9` discards the record accumulator, losing records `send()` accepted.

</details>

**5. Every dashboard is green. Lag is zero. Consumer group has three healthy members. Records are not reaching your database. Name two causes consistent with all of that.**

<details>
<summary>Reveal answer</summary>

**Dead-lettering.** Every record fails, exhausts its retries, and is published to `wikimedia-stream.dlt`. The offset is committed — from the source topic's perspective this is success — so lag stays at zero and members stay healthy. Records land in a topic nobody consumes, and vanish in 30 days (Lesson 21).

**A `groupId` collision.** A second application deployed with the same `group.id` joined the group and took some partitions. Each app now processes a subset of records. Both look healthy, lag is zero across the group, and your database is missing roughly half the events. Nothing errors (Lesson 15).

A third: the consumer acknowledges *before* the database write, so `save()` failures are logged and skipped while offsets advance (Lesson 18).

All three share a shape — **the pipeline is successfully processing records into nowhere.** Lag measures whether you're keeping up, not whether you're doing the right thing. That's why you also alert on DLT produce rate, member count, and a business-level metric such as rows written per minute.

</details>

---

## You're done

Twenty-eight lessons ago you didn't have a topic. Now you have:

- a 3-broker KRaft cluster you can inspect, break, and heal
- a producer that batches, compresses, keys, and never silently drops a record
- a consumer that persists before it commits, so an outage becomes lag rather than loss
- retries that back off, a classification that doesn't waste them, and a dead-letter topic that catches what's left
- integration tests against a real broker, covering the failure path as well as the happy one
- a schema contract that rejects breaking changes at the producer's startup
- three telemetry signals, and the knowledge that a green dashboard proves nothing until you've sent a probe through it

More usefully, you can reason about the failures. You know why a keyless producer costs you ordering forever, why `acks=all` alone is not durability, why a slow listener causes duplicate work rather than merely lag, and why a dead-letter topic makes an incident quieter rather than louder.

---

## Where to go next

**Kafka Streams** — stateful stream processing: joins, windows, aggregations, with state stores backed by changelog topics. The natural sequel to `@KafkaListener`.

**Kafka Connect** — moving data in and out of Kafka without writing consumers. Debezium's CDC connector, in particular, turns your database's write-ahead log into a topic.

**Transactions and exactly-once** — `read_committed` (Lesson 17) exists for this. Worth understanding, worth avoiding until at-least-once plus idempotency genuinely isn't enough.

**Tiered storage** — offloading old segments to object storage, decoupling retention from broker disk. It changes the "retention is expensive" calculus that shaped several decisions in this course.

---

Back to the **[course index](README.md)**, or the [main README](../README.md) for the reference implementation.
