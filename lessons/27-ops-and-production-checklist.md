# Lesson 27: Ops Toolbox and Production Checklist

> **Part 5: Production**

---

## What you'll learn

- The CLI commands worth knowing by heart when something is wrong
- How to diagnose the four failures this course has caused on purpose
- Why a producer needs a shutdown hook, and what it protects
- What separates the pipeline you built from one you would trust with money

---

## Why this matters

Every lesson so far introduced a mechanism. This one is about the fifteen minutes after something breaks, when nobody cares how partitions are assigned and everyone wants to know which records are missing.

It is also an honest accounting. You have built a complete, working pipeline that is not production-ready, and the gap between those two things is worth naming precisely rather than gesturing at.

---

## Before you start

[Lesson 26](26-observability.md), with a running stack.

Every command below runs inside a container and uses the internal listener, for the reason Lesson 06 demonstrated with real warnings:

```bash
docker exec kafka-1 <command> --bootstrap-server kafka-1:29092 ...
```

---

## The concept

### The toolbox

Six commands cover most of what you will need.

**Cluster health.**

```bash
docker exec kafka-1 kafka-metadata-quorum \
  --bootstrap-server kafka-1:29092 describe --status
```

`LeaderId` of `-1` means no controller, which means metadata changes have stopped: no leader elections, no topic creation, no ISR updates.

**Under-replicated partitions**, the single best one-line health check:

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --describe --under-replicated-partitions
```

Empty output is good. Anything listed means a replica has fallen out of an ISR, so your durability margin has shrunk and you may be close to refusing writes.

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --describe --unavailable-partitions
```

Anything listed here has no leader at all, and is neither readable nor writable.

**Consumer group state.**

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 --describe --group wikimedia-consumer-group
```

Read three things: per-partition `LAG`, whether members are present, and whether one partition is behind while the others are fine. That last pattern is a poison pill or a hot partition, and it is invisible in aggregate lag.

**Topic configuration**, when you want to know what a topic actually has rather than what your code says:

```bash
docker exec kafka-1 kafka-configs \
  --bootstrap-server kafka-1:29092 --entity-type topics \
  --entity-name wikimedia-stream --describe --all
```

Read the `synonyms` field. Lesson 06 used this to discover a Compose setting that a dynamic default was silently overriding.

**Where a topic's records are.**

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream --time -1
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream --time -2
```

`-1` is the end offset and `-2` is the start. The difference is how many records you can currently read, which is not the same as how many were ever written.

**Preferred leader election**, for the skew Lesson 02 described:

```bash
docker exec kafka-1 kafka-leader-election \
  --bootstrap-server kafka-1:29092 --election-type preferred --all-topic-partitions
```

After a broker restart its partitions stay led by whoever took over. This hands leadership back to the first entry in each `Replicas` list, evening out the load.

### Graceful shutdown

A producer holds unsent records in a buffer, as Lesson 12 established. Kill the process and that buffer is discarded, so records your application believed it had sent are gone.

Spring closes the context on `SIGTERM`, which flushes the `KafkaTemplate`. Two things defeat that: `SIGKILL`, which runs nothing, and a container orchestrator whose termination grace period is shorter than your flush.

An explicit hook makes the intent visible and gives you somewhere to log:

```java
package com.example.wikimedia.producer.kafka;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WikimediaProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class);
    private static final String TOPIC = "wikimedia-stream";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String message) {
        kafkaTemplate.send(TOPIC, key, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send key={}: {}", key, ex.getMessage());
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.debug("Sent topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                });
    }

    /**
     * Blocks until every buffered record has been acknowledged. Without this,
     * a shutdown discards whatever linger.ms was still waiting to batch.
     */
    @PreDestroy
    public void flushOnShutdown() {
        log.info("Flushing producer buffer before shutdown");
        kafkaTemplate.flush();
    }
}
```

On the consumer side the equivalent concern is that a shutdown mid-record leaves the offset uncommitted, so the record is redelivered. That is not a problem, because Lesson 18 made processing idempotent. It is the reason that lesson mattered.

---

## Hands-on

### Recipe 1: lag is climbing

Confirm it is real, and find whether it is one partition or all of them:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 --describe --group wikimedia-consumer-group
```

**All partitions climbing, members present.** Consumers are slower than producers. Check whether concurrency matches the partition count, and remember the partition count is the ceiling.

**All partitions climbing, no members.** The consumer is down. Lag was the wrong alert on its own, which is why Lesson 05's second quiz question exists.

**One partition climbing.** Either a poison pill, if the offset is frozen, or a hot partition, if it is advancing slowly. Lesson 20 showed the first and Lesson 11 the second.

### Recipe 2: writes are being refused

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --describe --under-replicated-partitions
```

If a partition appears with an ISR below `min.insync.replicas`, `acks=all` writes are being rejected with `NotEnoughReplicasException`. That is Kafka choosing to be unavailable rather than accept a write it cannot make durable.

Then check whether the cluster is otherwise healthy:

```bash
docker exec kafka-1 kafka-metadata-quorum \
  --bootstrap-server kafka-1:29092 describe --status | grep LeaderId
```

A real node ID means one broker is missing. A `-1` means you have lost quorum as well, and you are looking at two failures at once.

### Recipe 3: the consumer is running and nothing is arriving

The nastiest one, because every graph looks fine.

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream.dlt --time -1
```

If those offsets are climbing, the consumer is receiving records and rejecting all of them. Lag is zero because dead-lettering advances the offset, exactly as Lesson 21 and Lesson 22 established.

Then read one:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream.dlt \
  --from-beginning --max-messages 1 \
  --formatter-property print.headers=true
```

And read `kafka_dlt-exception-cause-fqcn`, not `kafka_dlt-exception-fqcn`. The primary header holds Spring's `ListenerExecutionFailedException` on every record, so filtering on it tells you nothing.

### Recipe 4: a consumer keeps rebalancing

Look for `CommitFailedException` in your application logs. If it is there, a member is being evicted mid-processing and its partitions reassigned.

The cause is processing slower than `max.poll.interval.ms`, not heartbeating. Raising `session.timeout.ms` will not help, for the reason Lesson 19 gave: heartbeats come from a separate thread and were never the problem.

Fix it in this order: make processing faster, lower `max-poll-records` so one poll's workload is smaller, then raise `max.poll.interval.ms`.

### Recipe 5: a schema deploy went out and nothing publishes

Health checks are green and the producer cannot send. Look for `SerializationException` with an HTTP 409.

```bash
curl -s localhost:8085/subjects/wikimedia-stream-value/versions
```

Registration is lazy, inside `serialize()`, so an incompatible schema starts cleanly and fails on the first record. That is Lesson 25's point, and it is why a CI compatibility check is worth having.

---

## Try it yourself

1. Cause each of the five recipes deliberately, and time how long it takes you to diagnose each one using only the CLI. Which was hardest, and which metric would have told you fastest?

2. Restart one broker, then check leader skew in Kafka UI. Run the preferred-leader election and check again. Quantify the difference.

3. Kill the producer with `SIGKILL` while the stream is running, with `linger.ms` at 20, then again with `SIGTERM`. Compare the end offsets before and after each. Does `@PreDestroy` run in both cases?

4. Work through the checklist below against your own pipeline and write down which three items you would fix first if this were carrying real traffic. Defend the ordering.

---

## The production checklist

What separates what you have built from something you would trust with money. Every unticked box is a real gap, and they are ordered roughly by how much damage each would do.

### Security

- [ ] **TLS between clients and brokers.** Everything you built crosses the network in the clear on `PLAINTEXT` listeners.
- [ ] **Authentication**, with SASL or mTLS. Right now any process that can reach port 9092 is a fully privileged client.
- [ ] **ACLs per topic and per consumer group.** No client should be able to read a topic it does not need, or create topics at all.
- [ ] **Secrets outside source control.** The H2 password you set in Lesson 18 is in a committed file.
- [ ] **The H2 console disabled.** Lesson 18 enabled it for convenience.

### Durability

- [x] **Replication factor 3 with `min.insync.replicas` of 2 and `acks=all`.** Lesson 09 and Lesson 10.
- [x] **Idempotent producer.** Lesson 10.
- [x] **Manual acknowledgment after the write.** Lesson 17 and Lesson 18.
- [x] **Idempotent processing, keyed on the record's identity.** Lesson 18.
- [ ] **`unclean.leader.election.enable=false` verified**, not assumed. It is the default; confirm it.
- [ ] **Backups or tiered storage.** Retention is not a backup.

### Schema and data

- [x] **A schema registry with a compatibility rule.** Lesson 25.
- [ ] **Compatibility checked in CI**, not discovered on the first `send()`.
- [ ] **A retention policy that matches your replay requirement.** Lesson 09's fourth quiz question: `retention.bytes` and `retention.ms` are an OR.

### Operations

- [x] **Topics declared as code.** Lesson 09, with the caveat that it creates rather than converges.
- [ ] **Topic configuration managed by something that converges**, since `NewTopic` ignores changes to an existing topic.
- [x] **Auto topic creation disabled.** Lesson 09.
- [x] **Cooperative rebalancing.** Lesson 19. Note this is close to the default already: since Kafka 3.0 the default strategy list negotiates cooperative when all members support it.
- [x] **Graceful shutdown that flushes the producer.** This lesson.
- [ ] **A runbook**, so the five recipes above are not carried in one person's head.

### Observability

- [x] **Metrics, traces and logs.** Lesson 26.
- [x] **Alert on consumer lag.** Lesson 26.
- [x] **Alert on the dead-letter rate**, because dead-lettering clears lag.
- [ ] **Alert on under-replicated partitions.**
- [ ] **Trace sampling set for your volume.** 1.0 is fine locally and ruinous at scale.

### Testing

- [x] **Integration tests against a real broker.** Lesson 24.
- [x] **The dead-letter path covered by a test.** Lesson 24.
- [ ] **A test against a real database rather than H2.**
- [ ] **A load test that establishes your actual throughput ceiling.**

---

## Common mistakes

**Using `localhost:9092` inside a container.**
It reaches only that container's broker, and the metadata it returns points at addresses that do not resolve there.

**Filtering a dead-letter topic on `kafka_dlt-exception-fqcn`.**
Every record holds Spring's wrapper. Your exception is in the cause header.

**Alerting on lag alone.**
A consumer rejecting every record reports zero lag.

**Raising `session.timeout.ms` to stop rebalances.**
Wrong timeout. Slow processing trips `max.poll.interval.ms`.

**Treating retention as a backup.**
It is a deletion policy with an OR in it.

**Assuming `SIGKILL` flushes anything.**
It runs no shutdown hook. Buffered records are lost.

**Trusting your code over the broker.**
`kafka-configs --describe --all` is the truth. Lesson 06 found a setting that was being silently overridden.

---

## Check your understanding

**1. Aggregate lag is flat and a downstream team says records are missing. Where do you look?**

<details>
<summary>Reveal answer</summary>

Per-partition lag first, then the dead-letter topic.

Aggregate lag hides two failures. A single partition frozen by a poison pill contributes a growing number to a sum that is otherwise small, and on a busy topic that can look like noise. And a consumer failing every record has zero lag by construction, because the recoverer advances the offset.

So: `--describe` the group and look for one partition behaving differently, then check whether the dead-letter topic's end offsets are climbing. If they are, the pipeline is working perfectly and rejecting everything.

</details>

**2. Why will raising `session.timeout.ms` not fix a rebalance storm caused by slow processing?**

<details>
<summary>Reveal answer</summary>

Because it governs heartbeats, and heartbeats were never late.

Heartbeats come from a background thread that keeps beating regardless of what your listener is doing, so a member spending six minutes on one record is heartbeating perfectly throughout. What evicted it is `max.poll.interval.ms`, which measures the gap between `poll()` calls.

Raising the session timeout also has a cost: genuinely dead members now take that much longer to be detected, so real failures recover more slowly. You have made the wrong thing worse.

</details>

**3. You `SIGKILL` the producer with `linger.ms` at 20. What is lost?**

<details>
<summary>Reveal answer</summary>

Every record still in the accumulator, which under light load is up to 20 milliseconds' worth per partition.

`SIGKILL` runs no shutdown hook, so `@PreDestroy` never fires, the context never closes and nothing flushes. Any record whose batch had not yet been sent is discarded from memory.

The application had already returned from `send()` for those records, so nothing in your code sees a failure. Lesson 13's callback fires only for records that got as far as a broker response, and these never did.

This is also why `acks` cannot help. `acks` is a promise about what the broker does once a record arrives, and these records never left the process.

</details>

**4. `MANUAL_IMMEDIATE` commits after each record. You `SIGKILL` the consumer immediately after a database write. Is the record reprocessed?**

<details>
<summary>Reveal answer</summary>

It depends on whether `acknowledge()` had returned, and the answer is more favourable than people expect.

`MANUAL_IMMEDIATE` uses `commitSync`, because Spring's `syncCommits` defaults to true. So `acknowledge()` blocks until the broker confirms the offset. If it returned before the kill, the offset is genuinely stored and the record is not redelivered.

If the kill landed between the database write and the commit returning, the record is redelivered and written again, which Lesson 18's unique constraint absorbs.

The window is real but small, and it is synchronous rather than fire-and-forget. Descriptions of `MANUAL_IMMEDIATE` as an async commit have the mechanism backwards, and the practical consequence is that this mode costs a round trip per record.

</details>

**5. A schema deploy leaves the producer healthy and unable to publish. Why did no health check catch it?**

<details>
<summary>Reveal answer</summary>

Because `KafkaAvroSerializer` registers the schema lazily, inside `serialize()`, on the first record it handles.

Nothing contacts the registry during startup, so context initialisation, health endpoints and readiness probes all pass. The failure arrives with the first record as a `SerializationException` wrapping an HTTP 409, at which point the deploy has been declared successful.

Setting `auto.register.schemas` to false changes who may register, not when the lookup happens, so it does not move the failure earlier either.

Catching this at deploy time requires checking compatibility yourself, either in CI against the registry's compatibility endpoint or in a startup bean that fails the context. That is the unticked checklist item under Schema and data.

</details>

---

## Recap

Six CLI commands cover most incidents: quorum status, under-replicated and unavailable partitions, consumer group state, effective topic configuration, offsets, and preferred-leader election.

The five recipes map symptoms to causes, and three of them exist because a metric you would naturally trust is structurally unable to see the problem: aggregate lag hides a single stuck partition, zero lag hides a consumer rejecting everything, and a green health check hides a lazy schema registration.

A producer needs an explicit flush on shutdown, because buffered records are invisible to `acks` and to your callbacks.

---

## End of Part 5

You started with one broker and no code. You now have a pipeline that streams live events from a public feed, keys them so that related records stay ordered, publishes them durably to a replicated topic, consumes them on three threads, persists them idempotently, retries transient failures with backoff, parks permanent ones with their payload and diagnosis intact, serves them over an HTTP API, verifies itself against a real broker in tests, versions its payload with a schema registry, and reports all three observability signals.

More importantly, you can explain why each of those decisions was made, and what breaks without it.

## Where to go next

Three directions, in increasing distance from what you have built.

**Harden this.** Work down the checklist. TLS, SASL and ACLs are the largest single gap, and they change every client's configuration.

**Kafka Streams.** Everything here treated Kafka as transport. Streams treats it as a database you can join, aggregate and window, with state stores backed by compacted topics. The mental model you built in Lesson 01 is the prerequisite.

**Kafka Connect.** Most pipelines that move data between Kafka and something else do not need an application at all. Connect is the framework for that, and knowing when not to write a consumer is worth as much as knowing how.

Back to the **[course index](README.md)**.
