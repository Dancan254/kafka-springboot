# Lesson 17 — Manual Acknowledgment

> **Part 3 — The Consumer** · 30 minutes

---

## What you'll learn

- How auto-commit loses records, and how it duplicates them
- What `AckMode.MANUAL_IMMEDIATE` changes, and when `acknowledge()` must be called
- Why at-least-once plus idempotent processing beats chasing exactly-once
- What `isolation.level: read_committed` actually filters

---

## Why this matters

A committed offset is a claim: *"everything before this has been processed."* Auto-commit makes that claim on a five-second timer, without asking whether it's true.

That single fact is the source of most "Kafka lost my data" and "Kafka duplicated my data" stories. Both are real, both are caused by the same setting, and which one you get depends only on where your consumer happens to crash.

This is the lesson that decides your pipeline's delivery semantics.

---

## Before you start

[Lesson 16](16-dtos-and-deserialization.md). A consumer that parses events.

---

## The concept

### Auto-commit is a timer, not a promise

With `enable.auto.commit=true` (the default), the consumer commits the offsets returned by the *last* `poll()` every `auto.commit.interval.ms` — 5 seconds — from within the poll loop.

Note what it does **not** consult: whether your listener method finished, succeeded, or ran at all.

**How it loses records.** A poll returns records 100–199. The commit timer fires and commits offset 200. Your listener is on record 105 when the process is killed. On restart the group resumes at 200. **Records 105–199 were never processed and never will be.** Kafka's offset says they were.

**How it duplicates records.** A poll returns records 100–199. Your listener processes all of them, writing to a database. The process is killed at record 199, before the commit timer fires. On restart the group resumes at 100. **Records 100–198 are processed a second time.**

You get data loss or duplication, and you don't choose which. You just find out.

### Manual acknowledgment

Turn the timer off:

```yaml
enable-auto-commit: false
```

and take an `Acknowledgment` parameter:

```java
public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
    process(record);     // if this throws, we never reach the next line
    ack.acknowledge();   // commit only now
}
```

The offset advances only after processing succeeded. If `process()` throws, no commit happens, the record is redelivered, and — with the error handler from Lesson 20 — retried or routed to a dead-letter topic.

The ordering is the entire point:

> **Do the work. Then commit. Never the reverse.**

### Ack modes

Spring's `ContainerProperties.AckMode` controls when a commit is actually sent:

| Mode | Commits |
|---|---|
| `BATCH` (default) | after all records from one `poll()` are processed |
| `RECORD` | after each record's listener returns |
| `MANUAL` | when you call `acknowledge()`, queued until the poll loop ends |
| `MANUAL_IMMEDIATE` | when you call `acknowledge()`, sent right away |

`MANUAL` and `MANUAL_IMMEDIATE` both require an `Acknowledgment` parameter. The difference is latency: `MANUAL` batches the commit until the current poll's records are done; `MANUAL_IMMEDIATE` issues the commit on the consumer thread immediately.

This project uses `MANUAL_IMMEDIATE`. The offset in `__consumer_offsets` reflects reality within milliseconds, which shortens the duplicate window if the process dies right after a database write. You pay one commit request per record.

> `MANUAL_IMMEDIATE` does not mean *synchronous*. It's `commitAsync` under the hood, issued at once rather than deferred.

### At-least-once, and why that's fine

Manual ack gives you **at-least-once** delivery: every record is processed one or more times.

Consider the unavoidable gap:

```
1. process(record)      → row written to database
2. ← process crashes here
3. ack.acknowledge()    → never runs
```

On restart, the record is redelivered and the row is written again. You cannot close this gap by reordering — committing first gives you at-most-once (data loss) instead. There is no ordering of two non-atomic operations that yields exactly-once.

The fix isn't better ordering. It's making step 1 **idempotent**: applying it twice has the same effect as applying it once.

For this project, the natural idempotency key is `(kafkaPartition, kafkaOffset)` — a unique identifier for the record, which is exactly why the entity in Lesson 18 stores both. A unique constraint on that pair turns the duplicate write into a no-op.

> **at-least-once + idempotent processing = effectively-once**
>
> This is what almost every production Kafka pipeline actually does. It is simpler, faster, and more robust than transactions, and it works when your sink is a database rather than another Kafka topic.

### Exactly-once, honestly

Kafka *does* offer exactly-once semantics, via transactional producers and `isolation.level=read_committed`. It works, with two caveats worth stating plainly:

**It only covers Kafka-to-Kafka.** A transaction spans a consume, a process, and a produce *back to Kafka*, atomically committing offsets and output records together. A write to PostgreSQL is not in the transaction and cannot be.

**It costs throughput.** Transaction coordination, markers in the log, and consumers buffering until commit.

### `isolation.level`

```yaml
isolation.level: read_committed
```

This is a **consumer** setting, and it filters records written by **transactional producers**:

- `read_uncommitted` (default) — deliver every record, including ones from transactions that later aborted
- `read_committed` — deliver only records from committed transactions; skip aborted ones

If no producer on the topic uses transactions, this setting changes nothing you can observe. This project sets it anyway: it's harmless, and it means that if a transactional producer ever writes to `wikimedia-stream`, the consumer will not process records from rolled-back transactions.

One real consequence: with `read_committed`, a consumer cannot read past an open transaction (the *last stable offset*). A long-running transaction stalls consumers, and lag climbs even though records are being produced.

---

## Hands-on

### 1. Turn off auto-commit

`application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092,localhost:9093,localhost:9094

    consumer:
      group-id: wikimedia-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

      # The offset advances only when our code says so.
      enable-auto-commit: false

      max-poll-records: 500

      properties:
        # Only meaningful if a transactional producer writes to this topic.
        # Costs nothing, and prevents ever reading an aborted transaction's records.
        isolation.level: read_committed

        # The broker evicts a member that stops heartbeating for this long.
        session.timeout.ms: 45000
        heartbeat.interval.ms: 15000

        # The broker evicts a member that stops *polling* for this long.
        # Slow processing trips this one, not session.timeout.ms.
        max.poll.interval.ms: 300000
```

### 2. Configure the ack mode

`enable-auto-commit: false` stops the timer, but Spring still needs telling *when* to commit. That lives on the listener container factory, not in YAML.

Create `config/KafkaConsumerConfig.java`:

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
     * Declaring this bean replaces Spring Boot's auto-configured factory.
     * The ConsumerFactory is still auto-configured from spring.kafka.consumer.*,
     * but every spring.kafka.listener.* property is now ignored — this factory
     * owns all listener-level settings.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        return factory;
    }
}
```

Read that Javadoc twice. The moment you declare your own `kafkaListenerContainerFactory` bean, **`spring.kafka.listener.ack-mode` and every other `spring.kafka.listener.*` property stops having any effect.** Boot's auto-configuration backs off. Setting `ack-mode: manual_immediate` in YAML alongside this bean does nothing, and there is no warning.

This is the most common way people configure manual ack, restart, and find offsets still committing on a timer.

### 3. Acknowledge explicitly

```java
package com.javaguy.consumer.consumer;

import com.javaguy.consumer.dto.WikimediaEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class WikimediaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaConsumer.class);

    private final ObjectMapper objectMapper;

    public WikimediaConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "wikimedia-stream",
            groupId = "wikimedia-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        WikimediaEventDto dto = parse(record);

        log.info("Consumed | partition={} offset={} type={} wiki={} title='{}'",
                record.partition(), record.offset(), dto.type(), dto.wiki(), dto.title());

        // Commit only after the record has been fully handled. If anything above
        // throws, this line is never reached, the offset is not advanced, and the
        // record is redelivered.
        acknowledgment.acknowledge();
    }

    private WikimediaEventDto parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), WikimediaEventDto.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException(
                    "Unparseable Wikimedia event [partition=%d offset=%d]: %s"
                            .formatted(record.partition(), record.offset(), e.getMessage()), e);
        }
    }
}
```

### 4. Prove the offset doesn't move on failure

Stop the consumer. Note its committed offsets:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group wikimedia-consumer-group
```

Now temporarily make every record fail, right before the ack:

```java
        if (true) throw new IllegalStateException("simulated failure");
        acknowledgment.acknowledge();
```

Run the consumer for ten seconds, stop it, and describe the group again.

**`CURRENT-OFFSET` has not moved.** The listener ran, threw, and never acknowledged. Kafka's record of your progress is unchanged, because your progress was zero.

Now do the same with `enable-auto-commit: true` (delete the `Acknowledgment` parameter and the ack). Run, throw on every record, stop, describe.

**The offset advanced.** Every record was "processed" according to Kafka. None of them were. You have just lost every record in that window, permanently, with no error visible anywhere except the exception logs nobody was reading.

Remove the simulated failure and restore `enable-auto-commit: false`.

### 5. See the duplicate window

Restore the working listener, add a `Thread.sleep(2000)` between the log line and the `acknowledge()`, and start the consumer. While it's sleeping on a record, kill it with `Ctrl-C`.

Restart. That record is delivered again — you'll see the same partition and offset logged twice.

That's at-least-once, and there is no configuration that removes it. Only idempotent processing does, which is exactly what the next lesson builds.

---

## Try it yourself

1. Set `AckMode.RECORD` and remove the `Acknowledgment` parameter. Does it compile? Does it behave like manual ack? What's the difference in the crash window between `RECORD` and `MANUAL_IMMEDIATE`?

2. Keep `enable-auto-commit: false` but *never* call `acknowledge()`. Run for a minute, stop, restart. What happens, and how many times will a given record be delivered over the application's lifetime?

3. Add `spring.kafka.listener.ack-mode: batch` to `application.yml` while `KafkaConsumerConfig` declares the factory with `MANUAL_IMMEDIATE`. Which wins? How would you have discovered that without this lesson telling you?

---

## Common mistakes

**Setting `spring.kafka.listener.*` while declaring your own container factory.**
Silently ignored. Boot's auto-configured factory — the one those properties bind to — isn't used.

**Calling `acknowledge()` first, then processing.**
That's at-most-once. A crash mid-processing loses the record permanently, and the offset says otherwise.

**Calling `acknowledge()` in a `finally` block.**
Commits even when processing threw. You've reinvented auto-commit, with extra steps.

**Assuming manual ack prevents duplicates.**
It prevents *loss*. Duplicates are inherent to at-least-once. Handle them with an idempotency key.

**Assuming `isolation.level: read_committed` deduplicates records.**
It filters records from *aborted transactions*. With no transactional producer, it does nothing.

**Confusing `session.timeout.ms` with `max.poll.interval.ms`.**
The first covers heartbeats (a separate thread); the second covers time between polls. Slow processing trips the second one, and no amount of raising the first will help.

---

## Check your understanding

**1. Auto-commit runs every 5 seconds. Your consumer processes a record, writes to the DB, and crashes 1 second later. Was data lost, duplicated, or neither?**

<details>
<summary>Reveal answer</summary>

Duplicated — the record is reprocessed on restart.

The commit timer hadn't fired, so the offset still points at that record. On restart it's redelivered and written to the database a second time.

Had the crash happened 6 seconds later, after the timer fired, there'd be no duplicate. Same code, same crash, different outcome based purely on clock timing. That non-determinism is the real indictment of auto-commit: you can't reason about it.

Note that manual ack doesn't fix *this* case either — the crash is between the DB write and the commit. What manual ack fixes is the *loss* case, where auto-commit advances past records the listener never touched.

</details>

**2. Why is "process, then acknowledge" strictly better than "acknowledge, then process", given that both can produce a wrong result on a crash?**

<details>
<summary>Reveal answer</summary>

Because their failure modes are not equally bad.

*Process then ack* fails toward **duplication**: the work happened, the commit didn't, so it happens again. Duplication is detectable and repairable — a unique constraint, an upsert, or a dedup key makes it a no-op.

*Ack then process* fails toward **loss**: the offset advanced, the work never happened, and the record is gone from your consumer's future forever. There is no local information that a record was skipped. Recovery means resetting offsets and reprocessing a window, if you even notice.

You cannot eliminate the gap between two non-atomic operations. You can choose which side of it you fail on. Always choose the recoverable one.

</details>

**3. Your consumer takes 8 minutes to process one record. `session.timeout.ms=45000`, `heartbeat.interval.ms=15000`, `max.poll.interval.ms=300000`. What happens, and which setting is responsible?**

<details>
<summary>Reveal answer</summary>

The consumer is evicted from the group after 5 minutes, by **`max.poll.interval.ms`**.

`session.timeout.ms` is not the culprit. Heartbeats are sent by a *background thread*, independently of your listener, so the broker keeps seeing the member as alive for the whole 8 minutes.

But the broker also requires the member to call `poll()` at least every `max.poll.interval.ms`. Your listener is blocking the poll loop, so at 5 minutes the coordinator concludes the member is stuck, removes it, and rebalances its partitions to another consumer — which reprocesses from the last committed offset while your original consumer is still working.

When the slow consumer finally finishes and tries to commit, it gets `CommitFailedException` because it no longer owns the partition.

The fix is to make processing faster, lower `max-poll-records`, or raise `max.poll.interval.ms` — not to touch `session.timeout.ms`.

</details>

**4. You use `MANUAL_IMMEDIATE`, process, then acknowledge. Records are still occasionally written twice to your database. Is something misconfigured?**

<details>
<summary>Reveal answer</summary>

No. That's at-least-once working as designed.

There is an irreducible window between the database write completing and the offset commit being durably recorded. A crash in that window means the record is redelivered, and your listener writes the row again.

Narrowing the window (which `MANUAL_IMMEDIATE` does, versus `MANUAL` or `BATCH`) makes it rarer. Nothing makes it zero, because the database write and the Kafka commit are two separate systems with no shared transaction.

The only real answer is to make the write idempotent — a unique constraint on `(partition, offset)`, or an upsert keyed on a business identifier — so a second write is a no-op. That's why the entity in Lesson 18 stores Kafka provenance columns.

</details>

**5. `isolation.level: read_committed` is set, and consumer lag suddenly climbs even though the consumer is healthy and CPU is idle. What might be happening?**

<details>
<summary>Reveal answer</summary>

A transactional producer probably has a long-running open transaction on one of your partitions.

Under `read_committed`, a consumer may not read past the **last stable offset** — the offset before the earliest still-open transaction. Records after it may belong to a transaction that ultimately aborts, so delivering them would be wrong.

So the broker withholds them. Your consumer polls, gets nothing, and lag grows because the log end offset keeps advancing while the consumer's position cannot. Nothing is broken in the consumer, and no error is logged anywhere.

Under `read_uncommitted` those records would be delivered immediately — and you'd risk processing records from a transaction that later rolled back. The stall is the price of the guarantee.

</details>

---

## Recap

Auto-commit advances offsets on a timer that knows nothing about your listener, and depending on where you crash it silently loses or duplicates records. Manual acknowledgment moves the commit to the one place it belongs: after the work succeeded. That buys you at-least-once delivery, and a duplicate window you close not with configuration but with idempotent processing.

`MANUAL_IMMEDIATE` must be set on your own container factory — and declaring that factory silently disables every `spring.kafka.listener.*` property.

Now let's do some work worth acknowledging.

**Next:** [Lesson 18 — Persisting with JPA →](18-persisting-with-jpa.md)
