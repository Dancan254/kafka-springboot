# Lesson 21 — Dead-Letter Topics

> **Part 4 — Resilience** · 30 minutes

---

## What you'll learn

- Why a dead-letter topic is the only alternative to blocking or losing records
- How `DeadLetterPublishingRecoverer` works, and how to control its destination
- Why the DLT should be routed to the *same partition number* as the source
- Why the DLT's retention should outlive the topic it shadows

---

## Why this matters

Lesson 20 left you with a bad trade. A record that exhausts its retries is logged and skipped — the partition unblocks, and the record is gone. Not deleted from Kafka, but never processed and never looked at again. Your lag returns to zero and your dashboards stay green while events silently vanish.

The three options for an unprocessable record are:

1. **Retry forever** — blocks the partition. Everything behind it stops.
2. **Skip it** — silent data loss. Nobody finds out.
3. **Park it somewhere** — the partition unblocks, and the record is preserved for a human.

Option 3 is a **dead-letter topic**. It is not a Kafka feature; it's an ordinary topic, and a convention.

---

## Before you start

[Lesson 20](20-error-handler-and-retries.md). An error handler with exponential backoff and non-retryable exceptions.

---

## The concept

### The recoverer

`DefaultErrorHandler` takes an optional second collaborator: a recoverer, invoked exactly once, when the backoff returns `STOP`.

```java
new DefaultErrorHandler(recoverer, backOff);
```

It's a `BiConsumer<ConsumerRecord<?, ?>, Exception>` — the record that failed, and why. What you do with it is up to you: write it to a table, call an alerting API, or publish it to another Kafka topic.

`DeadLetterPublishingRecoverer` does the last of these. It needs a `KafkaTemplate`, because publishing to Kafka means producing.

```java
new DeadLetterPublishingRecoverer(kafkaTemplate);
```

By default it publishes to a topic named `<original-topic>.DLT` — note the capitals — on a partition chosen by the producer's partitioner.

### Controlling the destination

The second constructor argument is a function from `(record, exception)` to a `TopicPartition`:

```java
new DeadLetterPublishingRecoverer(kafkaTemplate,
        (record, ex) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
```

Two decisions encoded there.

**Lowercase `.dlt`.** Cosmetic, but be consistent — `wikimedia-stream.DLT` and `wikimedia-stream.dlt` are different topics, and auto-creation will happily make both.

**`record.partition()`.** The failed record goes to *the same partition number* on the DLT that it occupied on the source topic. This is the interesting one.

### Why same-partition routing

Records that shared a partition on the source topic shared it because they shared a key (Lesson 04), which means they had a relative order that mattered.

If three records for the same key fail in sequence and land on three different DLT partitions, you have destroyed their order. Replaying them later — in whatever order three partitions happen to be read — could apply an update before the create it depends on.

Routing to the same partition number preserves per-partition ordering on the DLT side. Replay partition 1 of the DLT and you get the failures from partition 1 of the source, in the order they failed.

This requires the DLT to have **at least as many partitions as the source**. If the source has 3 and the DLT has 1, publishing to partition 2 fails. That's why `WikimediaTopicConfig` declares both with `.partitions(3)`.

> A subtlety: this preserves the order in which records *failed*, which is the source order only if failures are processed in order. With `setConcurrency(3)` each partition is single-threaded, so within a partition it holds.

### The headers you get for free

`DeadLetterPublishingRecoverer` attaches diagnostic headers to every record it publishes. The record's key and value are unchanged — the original bytes, preserved exactly.

| Header | Type | Content |
|---|---|---|
| `kafka_dlt-original-topic` | UTF-8 string | `wikimedia-stream` |
| `kafka_dlt-original-partition` | 4-byte big-endian int | source partition |
| `kafka_dlt-original-offset` | 8-byte big-endian long | source offset |
| `kafka_dlt-original-timestamp` | 8-byte big-endian long | source timestamp |
| `kafka_dlt-exception-fqcn` | UTF-8 string | exception class name |
| `kafka_dlt-exception-message` | UTF-8 string | exception message |
| `kafka_dlt-exception-cause-fqcn` | UTF-8 string | root cause class, if any |

That's a complete forensic record: what failed, why, and exactly where it came from. Note the types — they are **raw bytes**, not strings, for the numeric ones. Decoding them is Lesson 22.

### Retention: longer than the source

The source topic keeps 7 days. The DLT keeps **30**.

The asymmetry is deliberate. A record on the source topic is either processed within seconds or it isn't. A record on the DLT is waiting for a *human*: someone to notice the alert, read the exception, understand the bug, ship a fix, and replay.

Seven days is not enough for that. A failure on a Friday before a holiday needs to still be there when someone looks.

The DLT also sets `retention.bytes = -1` — no size cap at all. From Lesson 09: retention is an OR, and whichever limit trips first wins. A size cap on a DLT could silently delete failed records before the time window expires, which defeats the entire purpose.

### The DLT producer

Publishing to the DLT means the *consumer application* now produces. It needs producer configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092,...   # top level — shared

    consumer:
      ...
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
```

This is why `bootstrap-servers` lives at the top level rather than under `consumer:` — both the `ConsumerFactory` and the `ProducerFactory` inherit it.

`acks: all` on the DLT producer matters more than you'd think. If publishing to the DLT fails, the record is lost *and* you've lost the diagnostic. The one write you cannot afford to lose is the one recording that a write failed.

---

## Hands-on

### 1. Declare the DLT

Add to `config/WikimediaTopicConfig.java` in the **consumer** — the consumer owns the DLT, because the consumer is what writes to it:

```java
package com.javaguy.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class WikimediaTopicConfig {

    @Bean
    public NewTopic wikimediaStreamTopic() {
        return TopicBuilder
                .name("wikimedia-stream")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")      // 7 days
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "10737418240") // 10 GiB
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .build();
    }

    /**
     * Same partition count as the source so DeadLetterPublishingRecoverer can route a
     * failed record to the matching partition number, preserving per-key ordering.
     *
     * Retention is 30 days — a failed record waits for a human, not a retry. Size-based
     * eviction is disabled (-1) so nothing is deleted before that window expires.
     */
    @Bean
    public NewTopic wikimediaStreamDltTopic() {
        return TopicBuilder
                .name("wikimedia-stream.dlt")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")  // 30 days
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "-1")       // time-only eviction
                .build();
    }
}
```

### 2. Add producer config to the consumer

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
      enable-auto-commit: false
      max-poll-records: 500
      properties:
        isolation.level: read_committed
        session.timeout.ms: 45000
        heartbeat.interval.ms: 15000
        max.poll.interval.ms: 300000

    # Used only by DeadLetterPublishingRecoverer. This application produces to no
    # user-facing topic. acks=all because losing the record that records a failure
    # is the worst possible loss.
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
```

### 3. Wire the recoverer

`KafkaConsumerConfig`:

```java
package com.javaguy.consumer.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Routes a failed record to wikimedia-stream.dlt on the SAME partition number as
     * the source, so per-partition ordering survives on the DLT side.
     *
     * Spring Kafka adds diagnostic headers automatically — original topic, partition,
     * offset, timestamp, exception class, message, and root cause. See Lesson 22.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(
                        record.topic() + ".dlt",
                        record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        var backoff = new ExponentialBackOff(1_000L, 2.0);
        backoff.setMaxInterval(10_000L);
        backoff.setMaxElapsedTime(7_000L); // 1s + 2s + 4s → three retries, then recover

        var handler = new DefaultErrorHandler(recoverer, backoff);

        // Data-level failures never fix themselves. Skip the backoff and park the
        // record immediately rather than block the partition for 7 seconds first.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
```

The `KafkaTemplate<String, String>` bean comes from Spring Boot's auto-configuration, built from the `spring.kafka.producer.*` block you just added.

### 4. Trigger a non-retryable failure

Start the consumer. Produce garbage:

```bash
echo 'this is not json' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

```
ERROR ... Unparseable Wikimedia event [partition=1 offset=8423]
ERROR ... Backoff none exhausted for wikimedia-stream-1@8423
INFO  ... Saved | partition=1 offset=8424 ...
```

One attempt, no backoff, and the pipeline continues. Now look where the record went:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream.dlt \
  --from-beginning --max-messages 1 \
  --property print.partition=true
```

```
Partition:1	this is not json
```

**Partition 1** — the same partition it occupied on the source topic. The value is the original bytes, untouched.

### 5. Trigger a retryable failure

Add a temporary failure that is *not* on the non-retryable list:

```java
    if (record.value().contains("Nikola Tesla")) {
        throw new IllegalStateException("Simulated processing failure");
    }
```

Wait for a matching edit (or produce one by hand). Watch the timestamps: four attempts at 1 s, 2 s, 4 s, then the record lands on the DLT with `kafka_dlt-exception-fqcn = java.lang.IllegalStateException`.

Same destination, very different path. The retryable exception got its chance; the malformed one did not.

Remove the simulated failure.

### 6. Confirm the topic configuration

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic wikimedia-stream.dlt | head -1
```

```
Topic: wikimedia-stream.dlt  PartitionCount: 3  ReplicationFactor: 3
  Configs: min.insync.replicas=2,retention.ms=2592000000,retention.bytes=-1
```

Three partitions to match the source. Thirty days. No size cap.

---

## Try it yourself

1. Change the recoverer to route to partition `0` always. Produce three malformed records that would have gone to partitions 0, 1, and 2. What have you gained, and what have you given up? Now imagine replaying them.

2. Declare the DLT with `.partitions(1)` while the source has 3. Produce a record that fails on source partition 2. What happens inside `DeadLetterPublishingRecoverer`, and where does the record end up?

3. Stop all three brokers, then trigger a listener failure. Where does the DLT record go? What does this tell you about the DLT as a durability mechanism? (Hint: publishing to the DLT is a produce, and it can fail.)

4. Set the DLT's `retention.bytes` to `1048576` (1 MiB) alongside its 30-day `retention.ms`. Publish a few MB of failed records. Which limit wins, and why is this the worst possible topic to get that wrong on?

---

## Common mistakes

**Giving the DLT fewer partitions than the source.**
Same-partition routing throws. The recoverer's publish fails, and the failure of the failure-handler is not somewhere you want to be.

**Letting `DeadLetterPublishingRecoverer` use its default topic name.**
It appends `.DLT` (uppercase). If your topic config declares `.dlt`, auto-creation makes a second topic and your DLT consumer watches an empty one.

**Setting `acks=1` on the DLT producer.**
The record you cannot afford to lose is the one documenting a loss.

**Putting a `retention.bytes` cap on the DLT.**
Retention is an OR. Failed records get silently deleted before anyone investigates.

**Forgetting the DLT is a real topic that fills up.**
Nobody consumes it by default. It accumulates for 30 days. If your producer starts emitting malformed records at scale, the DLT absorbs the whole firehose.

**Assuming a DLT record means the data is safe.**
It means the data is *parked*. If nobody consumes and acts on the DLT, you have relocated your data loss by 30 days.

---

## Check your understanding

**1. Why route a failed record to the same partition number on the DLT rather than letting the partitioner choose?**

<details>
<summary>Reveal answer</summary>

To preserve relative ordering among failures.

Records sharing a source partition did so because they shared a key, which is how you asked Kafka to keep them ordered. If three failures for the same key scatter across three DLT partitions, they become three independent logs with no order between them.

Replaying them would then apply them in an arbitrary interleaving — potentially an update before its create. Same-partition routing means DLT partition 1 contains exactly the failures from source partition 1, in the order they failed, and a replay of that partition is ordered.

It's also why the DLT needs at least as many partitions as the source: you cannot publish to partition 2 of a one-partition topic.

</details>

**2. Your DLT has 30-day retention and `retention.bytes = 10485760` (10 MiB). A bad deploy sends 500 MiB of failures. What do you have 30 days later?**

<details>
<summary>Reveal answer</summary>

Roughly 10 MiB of the *most recent* failures, and the rest deleted — probably within minutes of the bad deploy, long before anyone investigated.

Retention is an OR: a segment is eligible for deletion when it exceeds *either* the time or the size limit. The size limit tripped first, so Kafka deleted the oldest segments while the incident was still unfolding.

The records you most want are the earliest ones — the first failures, closest to the root cause. Those are exactly the ones the size cap threw away.

This is why the DLT sets `retention.bytes = -1`. On a DLT, time should be the only thing that deletes a record. Set a size cap and you've built a mechanism that discards evidence precisely when there's the most of it.

</details>

**3. A record fails with `IllegalArgumentException` and one with `IllegalStateException`. Both end up on the DLT. What was different?**

<details>
<summary>Reveal answer</summary>

The path, not the destination.

`IllegalArgumentException` is registered via `addNotRetryableExceptions`, so the error handler skipped the backoff entirely — one attempt, then straight to the recoverer. The partition was blocked for milliseconds.

`IllegalStateException` isn't registered, so it got the full `ExponentialBackOff` schedule: four attempts at 0 s, 1 s, 2 s, 4 s. The partition was blocked for about seven seconds while the handler gave the record every chance to succeed.

The classification decides *how much you're willing to pay* before giving up. For a transient database outage, seven seconds is a bargain. For malformed JSON, it's seven seconds of blocked partition in exchange for four identical failures.

</details>

**4. Publishing to the DLT is itself a Kafka produce. What happens if it fails?**

<details>
<summary>Reveal answer</summary>

The recoverer throws, the error handler cannot complete, and the record is **not** committed — so the consumer will redeliver it and try the whole retry-and-recover sequence again on the next poll.

That's the good outcome, and it's why `acks=all` on the DLT producer matters: a failed publish is retried rather than silently dropped.

The bad outcome is the DLT being unavailable for an extended period — the brokers are down, or `min.insync.replicas` isn't satisfied. Then the consumer is stuck in a loop: fail, retry, fail to publish, redeliver. The partition blocks, exactly as it did before you had a DLT.

A dead-letter topic on the same cluster as the source topic does not protect you from that cluster being down. It protects you from *bad records*, which is a different failure class entirely. Treating it as a durability mechanism is a category error.

</details>

**5. Six months after adding a DLT, an engineer notices `wikimedia-stream.dlt` holds 40,000 records. Alerts never fired, lag was always zero. What went wrong, and where?**

<details>
<summary>Reveal answer</summary>

Nothing in the Kafka configuration. The DLT did exactly its job: it absorbed 40,000 unprocessable records, kept the partitions flowing, and preserved every one for inspection.

What failed is the human loop. A DLT is a **parking lot, not a solution**. Records land there and stay there. Consumer lag on the *source* topic stays at zero because those records were successfully removed from the source's path — that's the point.

So the monitoring gap: nobody alerted on the DLT's message count, or its produce rate, or ran a consumer over it. The 40,000 records represent 40,000 events that never reached the database, and after 30 days the oldest ones are gone forever.

The fix is to treat DLT arrivals as a paging-worthy event: a metric incremented on every publish, an alert on a non-zero rate, and a consumer that at minimum persists and reports them. Which is what Lesson 22 builds.

</details>

---

## Recap

`DeadLetterPublishingRecoverer` is the third option: instead of blocking the partition or silently dropping the record, park it on another topic with the original bytes intact and seven diagnostic headers attached. Route it to the same partition number to preserve ordering, give it thirty days and no size cap because it's waiting on a human, and use `acks=all` because losing the record of a loss is the worst outcome available.

Records are now landing on `wikimedia-stream.dlt`, and nobody is looking at them.

**Next:** [Lesson 22 — DLT headers & replay →](22-dlt-headers-and-replay.md)
