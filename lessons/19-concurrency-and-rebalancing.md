# Lesson 19 — Concurrency & Rebalancing

> **Part 3 — The Consumer** · 25 minutes

---

## What you'll learn

- What `setConcurrency(3)` actually creates, and why 4 would be pointless
- What triggers a rebalance, and what "stop-the-world" means for your throughput
- Why the cooperative sticky assignor exists
- How `max.poll.interval.ms` turns slow processing into a rebalance storm

---

## Why this matters

Your consumer reads three partitions on one thread. It works, and it caps your throughput at whatever one thread can do — while two thirds of your cluster's parallelism sits unused.

Raising concurrency is one line. Understanding what happens when a consumer joins, leaves, or stalls is the rest of this lesson, and it's the difference between a pipeline that scales and one that thrashes.

---

## Before you start

[Lesson 18](18-persisting-with-jpa.md). A consumer that persists events.

---

## The concept

### Concurrency is threads, bounded by partitions

```java
factory.setConcurrency(3);
```

`ConcurrentKafkaListenerContainerFactory` creates **N independent consumers**, each on its own thread, each with its own `KafkaConsumer` instance, all in the same group.

They are not a thread pool sharing work. Each is a full group member with its own poll loop and its own assigned partitions.

From Lesson 05: one partition goes to exactly one member. So with 3 partitions:

| Concurrency | Result |
|---|---|
| 1 | one thread reads all 3 partitions |
| 2 | one thread gets 2 partitions, one gets 1 |
| **3** | **one partition each — the sweet spot** |
| 4 | three work, one sits idle forever |

`setConcurrency(4)` on a 3-partition topic creates a thread that will never receive a record. It's not an error, and it costs you a consumer connection, a heartbeat, and a place in every rebalance.

**Partition count is the ceiling.** To scale beyond it you must add partitions — which, on a keyed topic, re-maps keys (Lesson 04).

### Thread safety becomes your problem

Three threads now run your listener concurrently. `WikimediaConsumer` is a singleton `@Service`.

It's safe here because it holds only `repository` and `objectMapper` — both thread-safe and stateless. Add a `private int counter` or a `List` field and you've introduced a race that appears under load and never in tests.

Records from the *same partition* are still processed by one thread, in order. Records from different partitions run in parallel. That's precisely the guarantee keys buy you (Lesson 11): same key → same partition → same thread → ordered.

### Rebalancing

A **rebalance** is the group coordinator reassigning partitions to members. It triggers on:

- a consumer **joining** (deploy, scale-up, `setConcurrency` increase)
- a consumer **leaving** cleanly (graceful shutdown)
- a consumer **failing to heartbeat** for `session.timeout.ms`
- a consumer **failing to poll** for `max.poll.interval.ms`
- **partition count changing** on a subscribed topic

The default protocol is **eager**: every member revokes *all* its partitions, the coordinator computes a new assignment, and everyone resumes. For the duration, **nobody consumes anything.** That's the "stop-the-world" rebalance.

On a small group it's milliseconds. On a large one, with heavy `onPartitionsRevoked` work, it can be seconds — during which lag grows across every partition, not just the ones that moved.

### Cooperative sticky: don't stop the world

`CooperativeStickyAssignor` changes this. Members keep partitions they'll retain, and only the partitions that genuinely need to move are revoked — in a second, cheap rebalance round.

```yaml
properties:
  partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Two properties, both valuable:

**Sticky** — a member tends to get back the partitions it had, so warmed caches and in-flight state survive.

**Cooperative** — unaffected members never stop consuming.

Rolling-restart a 10-member group under the eager assignor and you trigger 20 stop-the-world rebalances. Under cooperative sticky, the 9 unaffected members keep working through each one.

> Migrating a running group to cooperative requires a two-step rolling upgrade (both strategies listed, then the old one removed). You can't just flip it on a live group.

### The two timeouts, again

They are constantly confused, and they fail differently.

**`session.timeout.ms` (45 s here)** — the coordinator evicts a member that hasn't **heartbeat**. Heartbeats are sent by a *background thread* every `heartbeat.interval.ms` (15 s), independent of your listener. A slow listener does not miss heartbeats. This timeout catches crashed processes and network partitions.

**`max.poll.interval.ms` (5 min)** — the coordinator evicts a member that hasn't called **`poll()`**. Your listener runs *inside* the poll loop, so slow processing trips exactly this one.

The failure it produces is the nasty one:

1. Your listener takes 6 minutes on a batch.
2. At 5 minutes the coordinator evicts the member and reassigns its partitions.
3. Another consumer starts reprocessing them from the last committed offset — **duplicate work**.
4. Your original listener finishes and calls `acknowledge()`.
5. The commit fails with `CommitFailedException`: it no longer owns the partition.
6. The evicted member rejoins, triggering another rebalance.

Under sustained load this loops. It is a **rebalance storm**, and it looks like Kafka being flaky. It's your listener being slow.

The levers, in order of preference: make processing faster; lower `max-poll-records` so a poll's worth of work is smaller; raise `max.poll.interval.ms`. Raising `session.timeout.ms` does nothing at all.

---

## Hands-on

### 1. Set concurrency

Update `KafkaConsumerConfig`:

```java
package com.javaguy.consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Declaring this bean replaces Spring Boot's auto-configured factory, so every
     * spring.kafka.listener.* YAML property is ignored — this class owns them all.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);

        // One consumer thread per partition. A 4th would never receive a record.
        factory.setConcurrency(3);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);

        // Emits spans and metrics for each listener invocation (Lesson 26).
        factory.getContainerProperties().setObservationEnabled(true);

        return factory;
    }
}
```

### 2. Watch three threads appear

Add the thread name to your log pattern, or just log it:

```java
log.info("Saved | thread={} partition={} offset={}",
        Thread.currentThread().getName(), record.partition(), record.offset());
```

Run the consumer:

```
Saved | thread=wikimedia-consumer-group-0-C-1 partition=0 offset=483
Saved | thread=wikimedia-consumer-group-1-C-1 partition=1 offset=690
Saved | thread=wikimedia-consumer-group-2-C-1 partition=2 offset=257
Saved | thread=wikimedia-consumer-group-0-C-1 partition=0 offset=484
```

Three distinct threads. **Each thread only ever logs one partition number.** That's the one-partition-one-member rule, visible in your own output.

Confirm from outside:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group wikimedia-consumer-group
```

```
GROUP                    TOPIC            PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID
wikimedia-consumer-group wikimedia-stream 0         1322           1322           0   consumer-...-1
wikimedia-consumer-group wikimedia-stream 1         1098           1098           0   consumer-...-2
wikimedia-consumer-group wikimedia-stream 2         772            772            0   consumer-...-3
```

Three **distinct** `CONSUMER-ID`s from one JVM. Kafka doesn't know or care that they share a process.

### 3. Create an idle consumer

Set `setConcurrency(4)` and restart. Re-describe the group.

Four members, three partitions. One `CONSUMER-ID` owns nothing. It heartbeats, participates in every rebalance, and consumes zero records.

Set it back to 3.

### 4. Trigger a rebalance and watch it

Start the consumer with `setConcurrency(3)`. Then, in a second terminal, start a **second instance** on a different port:

```bash
SERVER_PORT=8083 ./mvnw spring-boot:run
```

Now the group has 6 members and 3 partitions. Describe it: three members own a partition each, three own nothing. Which three is arbitrary.

Watch the first instance's logs as the second joins:

```
Revoking previously assigned partitions [wikimedia-stream-0, wikimedia-stream-1, wikimedia-stream-2]
...
partitions assigned: [wikimedia-stream-1]
```

**It revoked all three, then got one back.** That's the eager protocol — stop the world, reassign everything, resume. During those milliseconds neither instance consumed anything.

Kill the second instance. Another rebalance; the first instance reclaims all three partitions.

### 5. Cause a rebalance storm

Add a slow operation to the listener, and shorten the leash so you don't wait five minutes:

```yaml
      max-poll-records: 10
      properties:
        max.poll.interval.ms: 10000    # 10 seconds
```

```java
        Thread.sleep(2000);   // 10 records × 2s = 20s per poll, over the 10s limit
        repository.save(event);
```

Run it and watch:

```
... Member consumer-... sending LeaveGroup request ... due to consumer poll timeout has expired.
... Attempt to heartbeat failed since group is rebalancing
... Offset commit cannot be completed since the consumer is not part of an active group
```

The consumer is evicted mid-batch, rejoins, gets partitions, starts processing, exceeds the interval again, is evicted again. Records are reprocessed each cycle. Lag climbs while the consumer is *busy*.

Note the third line: `acknowledge()` failed because the member no longer owned the partition. Work was done and could not be committed.

Now raise `max-poll-records` back to 500 and remove the sleep. Notice that raising `session.timeout.ms` would have changed nothing — heartbeats were never the problem.

### 6. Enable cooperative sticky

```yaml
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Repeat step 4 — start a second instance and watch the first instance's logs.

```
partitions revoked: [wikimedia-stream-2]
partitions assigned: []
```

It gave up **one** partition, not three. It kept consuming from the other two throughout. That's cooperative rebalancing.

---

## Try it yourself

1. Set `setConcurrency(3)` and run **two** instances. Six members, three partitions. Now kill one instance abruptly (`kill -9`, not Ctrl-C). How long until the surviving instance picks up the partitions, and which timeout governs that delay?

2. Add a `private int recordCount` field to `WikimediaConsumer` and increment it in the listener. Run with concurrency 3 under load, then compare the final count to `SELECT COUNT(*)`. Explain the discrepancy without using the word "race" — describe the exact interleaving.

3. Increase the topic to 6 partitions while the consumer is running. What happens to the group? Then set `setConcurrency(6)`. Did throughput double? What else did you change by adding partitions?

---

## Common mistakes

**Setting concurrency above the partition count.**
Extra threads idle forever, and still participate in every rebalance.

**Expecting concurrency to speed up a single partition.**
One partition, one member, always. Ordering depends on it.

**Putting mutable state in the listener bean.**
It's a singleton, now invoked from N threads.

**Raising `session.timeout.ms` to fix slow processing.**
Wrong timeout. Heartbeats are on a background thread. You want `max.poll.interval.ms`, or fewer `max-poll-records`.

**Flipping to `CooperativeStickyAssignor` on a live group in one deploy.**
Requires a two-phase rolling upgrade. A single flip can leave the group unable to agree on a protocol.

**Assuming a rebalance is free.**
Under the default eager assignor, every member stops consuming while it happens.

**Ignoring `CommitFailedException`.**
It means you processed records you no longer own, and someone else is reprocessing them.

---

## Check your understanding

**1. Topic has 3 partitions. You run two application instances, each with `setConcurrency(3)`. How many records does each instance process?**

<details>
<summary>Reveal answer</summary>

Roughly half — but not because each instance gets 1.5 partitions.

There are **6 members** in the group and 3 partitions. Kafka assigns each partition to exactly one member, so 3 members consume and 3 sit idle. The idle ones may all be in one instance, or split across both; the assignment is arbitrary.

So one instance might process everything while the other processes nothing. Or they might split 2–1. Nothing balances by *instance* — Kafka has no concept of your JVMs, only of group members.

To use both instances evenly you need at least 6 partitions.

</details>

**2. Why does a slow listener trip `max.poll.interval.ms` but never `session.timeout.ms`?**

<details>
<summary>Reveal answer</summary>

Because heartbeats and polling happen on different threads.

Since Kafka 0.10.1, the consumer sends heartbeats from a dedicated background thread every `heartbeat.interval.ms`, entirely independent of your listener. So no matter how long your code runs, the coordinator keeps seeing heartbeats and the session stays alive.

What it *doesn't* see is a call to `poll()`, because your listener is executing inside the poll loop and hasn't returned. `max.poll.interval.ms` exists precisely to catch that case: a member that is alive but not making progress.

The two timeouts detect different failures. `session.timeout.ms` detects a dead process. `max.poll.interval.ms` detects a live process that's stuck. Raising the former to fix a slow listener changes nothing.

</details>

**3. Your listener takes 6 minutes per poll batch. Trace what happens to the records it's working on.**

<details>
<summary>Reveal answer</summary>

At 5 minutes the coordinator evicts the member for exceeding `max.poll.interval.ms` and reassigns its partitions.

Another consumer picks them up and begins processing **from the last committed offset** — which is where the slow member started. Every record the slow member is currently working on is now being processed a second time, concurrently.

At 6 minutes the slow member finishes and calls `acknowledge()`. The commit fails with `CommitFailedException`, because it no longer owns the partition. Its six minutes of work is uncommitted and, if it had side effects, duplicated.

It then rejoins the group, triggering another rebalance, gets partitions, and starts another 6-minute batch. The cycle repeats indefinitely.

You get duplicate side effects, zero forward progress, and continuous rebalancing — while CPU looks busy and no component reports an error.

</details>

**4. Under the eager assignor, one consumer in a 10-member group restarts. How many members stop consuming?**

<details>
<summary>Reveal answer</summary>

All ten.

The eager protocol requires every member to revoke *all* of its partitions before the coordinator computes the new assignment. Nobody consumes anything until the assignment is distributed and everyone resumes — even the nine members whose partitions will be handed straight back to them.

That's the "stop-the-world" cost. A rolling restart of 10 pods means 20 of these (one on leave, one on rejoin), each pausing the entire group.

`CooperativeStickyAssignor` fixes exactly this: members keep partitions they'll retain, and only the partitions that must move are revoked. The other nine members never stop.

</details>

**5. You add a `private final List<String> seenTitles = new ArrayList<>()` to the listener and add each title to it. With `setConcurrency(3)`, what breaks?**

<details>
<summary>Reveal answer</summary>

`ArrayList` is not thread-safe, and the listener bean is a singleton invoked from three consumer threads simultaneously.

Concretely: `add()` may read `size`, be preempted, and have another thread write to the same array slot and increment `size` — so one title overwrites another and the list ends up shorter than the record count. Worse, a concurrent `grow()` during resizing can leave the backing array in a state that produces `ArrayIndexOutOfBoundsException`, or silently null entries.

None of this appears in a single-threaded test, and it appears rarely enough under light load to be dismissed as a fluke.

The `repository` and `objectMapper` fields are fine because both are thread-safe and stateless. The moment you add mutable state to a listener, concurrency becomes your problem — Spring will not warn you.

</details>

---

## Recap

`setConcurrency(N)` creates N independent group members, each with its own poll loop; the partition count caps how many can do work. Records from one partition are always handled by one thread, in order, which is what makes keys meaningful. Rebalances trigger on membership changes and stop the world under the default assignor — cooperative sticky avoids that. And slow processing trips `max.poll.interval.ms`, not `session.timeout.ms`, producing duplicate work and a rebalance storm.

---

## End of Part 3

Your consumer:

- joins a group, reads three partitions on three threads
- parses raw JSON into an immutable record, tolerating unknown fields
- persists to a database with Kafka provenance columns
- commits the offset only after the write succeeds, so an outage becomes lag rather than loss

The finished consumer lives at [`consumer/`](../consumer) in the repository root, if you want to diff your work against it.

And it still has the flaw from Lesson 16: feed it one malformed record and it retries forever, blocking a partition. Every valid record behind the poison pill waits.

Time to fix that properly.

**Next:** [Lesson 20 — `DefaultErrorHandler` & retries →](20-error-handler-and-retries.md)
