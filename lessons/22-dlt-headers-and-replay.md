# Lesson 22 — DLT Headers & Replay

> **Part 4 — Resilience** · 30 minutes

---

## What you'll learn

- What Kafka headers are, and why they're `byte[]` rather than strings
- How to decode the seven `kafka_dlt-*` headers, including the big-endian numeric ones
- Why a DLT consumer must never simply re-throw
- How to actually replay a dead-lettered record once you've fixed the bug

---

## Why this matters

Lesson 21 ended with records accumulating on `wikimedia-stream.dlt` and nobody watching. A dead-letter topic that nobody consumes is a 30-day countdown to data loss with extra steps.

Closing the loop needs three things: something that *notices* a record arrived, something that makes the failure *legible*, and a way to *replay* it once the bug is fixed. All three depend on reading headers — the metadata Spring attached on the way out, which is the only record of where the failure came from and why.

---

## Before you start

[Lesson 21](21-dead-letter-topics.md). A working DLT with at least one record on it.

If it's empty, put something there:

```bash
echo 'this is not json' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

---

## The concept

### Headers are bytes

A Kafka record is a key, a value, a timestamp, and **headers**: an ordered list of `(String name, byte[] value)` pairs.

Not `Map<String, String>`. Three consequences:

**Values are raw bytes.** Kafka does not know or care what's in them. A header holding the number `42` might be the four bytes `00 00 00 2A`, or the two ASCII characters `4` and `2`. You must know which.

**Names can repeat.** Headers are a list, not a map. `record.headers().lastHeader(name)` gets the most recent — which matters, because a record that fails, is dead-lettered, is replayed, and fails *again* accumulates a second set of `kafka_dlt-*` headers. The last one is the most recent failure.

**They travel with the record.** Headers are the standard place for cross-cutting metadata — trace IDs, schema versions, tenant IDs — that doesn't belong in your payload schema.

### The seven headers, and their types

`DeadLetterPublishingRecoverer` attaches these. Note the type column: it is the entire difficulty of this lesson.

| Header | Type | Decode with |
|---|---|---|
| `kafka_dlt-original-topic` | UTF-8 string | `new String(bytes, UTF_8)` |
| `kafka_dlt-original-partition` | **4-byte big-endian int** | `ByteBuffer.wrap(bytes).getInt()` |
| `kafka_dlt-original-offset` | **8-byte big-endian long** | `ByteBuffer.wrap(bytes).getLong()` |
| `kafka_dlt-original-timestamp` | **8-byte big-endian long** | `ByteBuffer.wrap(bytes).getLong()` |
| `kafka_dlt-exception-fqcn` | UTF-8 string | `new String(bytes, UTF_8)` |
| `kafka_dlt-exception-message` | UTF-8 string | `new String(bytes, UTF_8)` |
| `kafka_dlt-exception-cause-fqcn` | UTF-8 string | `new String(bytes, UTF_8)` |

Decode `kafka_dlt-original-partition` with `new String(...)` and you don't get `"1"`. You get the four bytes `00 00 00 01` interpreted as UTF-8 — three null characters and a `SOH` control character. It prints as garbage or as nothing at all, and it doesn't throw.

That silent-wrong-answer property is why this lesson exists. Java's `ByteBuffer` defaults to big-endian, which matches Kafka's wire format, so `ByteBuffer.wrap(h.value()).getInt()` is correct with no configuration.

### The value is untouched

The DLT record's key and value are **the original bytes**. `DeadLetterPublishingRecoverer` republishes them verbatim.

That's what makes replay possible: the payload on the DLT is byte-identical to the payload that failed. You can fix your consumer, read the record back off the DLT, and produce it to the source topic unchanged.

### A DLT consumer must not throw

This is the trap.

Your DLT listener has an error handler too — the same `DefaultErrorHandler` bean, since it's on the same container factory. If the DLT listener throws, the recoverer publishes the record to `wikimedia-stream.dlt.dlt`.

If *that* fails, you get `.dlt.dlt.dlt`. And a topic list nobody wants to explain.

So a DLT consumer's job is narrow and it must be reliable:

1. **Record** the failure — persist it, or at minimum log it with full context.
2. **Alert** — increment a metric someone is paged on.
3. **Acknowledge** — always. The record is already parked; failing to ack means redelivering it forever.

It must not do the thing that failed. It must not call the database that was down. It should do as close to nothing as possible.

### Replay, honestly

Replaying means producing the DLT record's value back to the source topic. There is no Kafka feature for this; you write it.

The shape:

```
read from wikimedia-stream.dlt
  → check the fix is deployed
  → produce value to wikimedia-stream
  → the main consumer processes it normally
```

Three things that make this harder than it sounds.

**Replaying a poison pill loops.** If you replay a malformed record without fixing the parser, it fails again and returns to the DLT. Now you have two copies. Replay is only safe *after* the fix.

**Replay is a new record.** It gets a new offset and a new timestamp on the source topic. Your idempotency key from Lesson 18 — `(partition, offset)` — will not match the original, so the record will be processed as new. If the original partially succeeded, you may double-apply. Idempotency on a *business* key is what saves you here.

**Order is not preserved across a replay.** The replayed record arrives at the end of the log, long after the records that originally followed it. If those depended on its effects, you have applied them in the wrong order and no amount of same-partition routing helps.

Replay is a repair tool, not a routine mechanism. Preventing the failure is better.

---

## Hands-on

### 1. Inspect the DLT from the CLI first

Before writing Java, look at what's actually there:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream.dlt \
  --from-beginning --max-messages 1 \
  --property print.headers=true \
  --property print.partition=true
```

```
Partition:1	kafka_dlt-original-topic:wikimedia-stream,kafka_dlt-original-partition:...,
kafka_dlt-exception-fqcn:java.lang.IllegalArgumentException,...	this is not json
```

The string headers are readable. `kafka_dlt-original-partition` is visibly mangled — that's the four raw bytes being printed as text. The console consumer has no idea it's an `int`.

### 2. Write the DLT consumer

`consumer/WikimediaDltConsumer.java`:

```java
package com.javaguy.consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Every record here was published by DeadLetterPublishingRecoverer after the main
 * consumer exhausted its retries.
 *
 * In production this method would: persist the failed record to a dead-letter store,
 * increment a metric that pages someone, and expose an endpoint to replay or discard.
 * It must never throw — a failure here routes the record to wikimedia-stream.dlt.dlt.
 */
@Service
public class WikimediaDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaDltConsumer.class);

    @KafkaListener(
            topics = "wikimedia-stream.dlt",
            groupId = "wikimedia-dlt-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.error(
                "[DLT] partition={} offset={} | originalTopic={} originalPartition={} "
                        + "originalOffset={} | exception={} cause='{}' | value='{}'",
                record.partition(),
                record.offset(),
                stringHeader(record, "kafka_dlt-original-topic"),
                intHeader(record, "kafka_dlt-original-partition"),
                longHeader(record, "kafka_dlt-original-offset"),
                stringHeader(record, "kafka_dlt-exception-fqcn"),
                stringHeader(record, "kafka_dlt-exception-message"),
                record.value()
        );

        acknowledgment.acknowledge();
    }

    private String stringHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? "n/a" : new String(header.value(), StandardCharsets.UTF_8);
    }

    // Kafka writes the partition as a 4-byte big-endian int. Reading it as a
    // String yields control characters, silently.
    private int intHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? -1 : ByteBuffer.wrap(header.value()).getInt();
    }

    // Offsets and timestamps are 8-byte big-endian longs.
    private long longHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? -1L : ByteBuffer.wrap(header.value()).getLong();
    }
}
```

Note the **separate `groupId`**. `wikimedia-dlt-consumer-group` is not `wikimedia-consumer-group`. From Lesson 05: two listeners in the same group on different topics would still share partition assignments and offsets in confusing ways. Different concerns, different groups.

Note also that every accessor returns a sentinel (`"n/a"`, `-1`) rather than throwing on a missing header. A DLT consumer that NPEs on an absent header has failed at the one job it has.

### 3. Run it

Restart the consumer. The DLT record is picked up immediately:

```
ERROR [DLT] partition=1 offset=0 | originalTopic=wikimedia-stream originalPartition=1
originalOffset=8423 | exception=java.lang.IllegalArgumentException
cause='Unparseable Wikimedia event [partition=1 offset=8423]: Unrecognized token 'this''
| value='this is not json'
```

Every question you'd want answered, in one line:

- **what** failed — `this is not json`
- **where it came from** — `wikimedia-stream`, partition 1, offset 8423
- **why** — `IllegalArgumentException`, with the parser's message
- **where it's parked now** — DLT partition 1, offset 0

`originalPartition=1` and the DLT's `partition=1` match. That's the same-partition routing from Lesson 21, verified.

### 4. Prove you can fetch the original record

The headers point back at the source. Use them:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream \
  --partition 1 \
  --offset 8423 \
  --max-messages 1
```

Substitute the offset from your own log. Out comes the original record, still sitting in the source topic where it always was.

**This is the payoff of the provenance headers.** Without `originalPartition` and `originalOffset`, you'd have a payload and no way to locate it in a topic holding millions of records.

### 5. Watch a `.dlt.dlt` topic appear

Make the DLT consumer throw:

```java
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        throw new IllegalStateException("DLT consumer is broken");
    }
```

Restart, then list the topics:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list
```

```
wikimedia-stream
wikimedia-stream.dlt
wikimedia-stream.dlt.dlt
```

There it is. The error handler applied to the DLT listener too, `IllegalStateException` got the full backoff, and the recoverer routed it onward — appending `.dlt` to a topic name that already ended in `.dlt`.

`wikimedia-stream.dlt.dlt` was auto-created with broker defaults: no `min.insync.replicas`, no 30-day retention, no size-cap removal. Every safety property you carefully configured is absent.

Clean up:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
  --delete --topic wikimedia-stream.dlt.dlt
```

Restore the working DLT consumer.

### 6. Replay a record

Fix your bug first — in this case, the "bug" is that `this is not json` was never valid, so there's nothing to fix. Simulate a real one: pretend the parser was broken and is now correct.

Read the DLT record's value and produce it back:

```bash
# Read the parked payload
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream.dlt \
  --from-beginning --max-messages 1 > /tmp/replay.txt

# Produce it back to the source topic
docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream < /tmp/replay.txt
```

Watch the consumer. Because the record is *still* unparseable, it fails, and returns to the DLT.

**You now have two copies of it on the DLT**, with two sets of headers pointing at two different source offsets.

That is the lesson. Replay without a fix multiplies the problem. Replay is a repair step that comes *after* a deploy, and a production replay endpoint should refuse to run unless someone asserts the fix is live.

---

## Try it yourself

1. Decode `kafka_dlt-original-partition` with `new String(header.value(), UTF_8)` instead of `ByteBuffer`. What prints? Does it throw? What does that tell you about how this bug would be discovered?

2. Add a Micrometer counter to the DLT consumer, incremented on every record, and expose it via actuator. Then write the PromQL alert you'd page on. Should it alert on the total, or the rate?

3. Replay a record that fails, twice. Then read the DLT record and call `record.headers().headers("kafka_dlt-exception-fqcn")` — plural, not `lastHeader`. How many are there? Which one is the current failure?

4. Persist DLT records to a `dead_letter_events` table instead of logging them. What columns do you need to make replay possible without ever reading Kafka again? Is the value column enough?

---

## Common mistakes

**Decoding `original-partition` or `original-offset` as a String.**
Silently produces control characters. No exception, no log, just a wrong value in your diagnostics.

**Throwing from the DLT consumer.**
Creates `<topic>.dlt.dlt`, auto-configured with broker defaults.

**Sharing a `groupId` between the main and DLT listeners.**
Two topics, one group, confusing assignments.

**Using `headers().lastHeader()` and assuming it's the only one.**
A replayed-and-refailed record has several. `lastHeader` is correct — but know *why* it's correct.

**Throwing on a missing header.**
`lastHeader()` returns `null` when absent. A DLT consumer that NPEs has failed at the one job it has.

**Replaying before deploying the fix.**
The record fails again and returns to the DLT, duplicated.

**Assuming a replayed record is idempotent because of `(partition, offset)`.**
A replay gets a *new* offset. That key won't match. You need a business-level idempotency key.

**Logging the DLT and calling it done.**
Nothing alerts on a log line. Increment a metric.

---

## Check your understanding

**1. Why is `kafka_dlt-original-partition` four bytes rather than the string `"1"`?**

<details>
<summary>Reveal answer</summary>

Because Kafka headers are `(String, byte[])` pairs, and Spring chose the natural binary encoding for a numeric value: a 4-byte big-endian `int`, the same width as Java's `int`.

It's compact and unambiguous — no charset, no parsing, no locale. `ByteBuffer.wrap(bytes).getInt()` reconstructs it exactly, and `ByteBuffer` defaults to big-endian, which matches Kafka's wire format everywhere.

The cost is that decoding it as UTF-8 does not fail. `new String(new byte[]{0,0,0,1})` produces a four-character string of control characters. It prints as whitespace, logs as nothing, and never throws. You get a wrong answer silently, which is the worst kind.

</details>

**2. Your DLT consumer throws. Trace exactly what happens.**

<details>
<summary>Reveal answer</summary>

The DLT listener uses the same `ConcurrentKafkaListenerContainerFactory`, so it has the same `DefaultErrorHandler` with the same backoff and the same `DeadLetterPublishingRecoverer`.

The exception is retried on the backoff schedule (four attempts, 1 s + 2 s + 4 s) unless it's registered non-retryable. When the backoff returns `STOP`, the recoverer runs. Its destination function is `record.topic() + ".dlt"` — and `record.topic()` is now `wikimedia-stream.dlt`.

So it publishes to `wikimedia-stream.dlt.dlt`, which doesn't exist, so the broker auto-creates it with defaults: no `min.insync.replicas=2`, 7-day retention rather than 30, and an active `retention.bytes` cap.

Every durability property you configured for the DLT is absent on the topic now holding your most-failed records. And if *that* consumer existed and threw, you'd get `.dlt.dlt.dlt`.

</details>

**3. You fix the consumer bug and replay 5,000 records from the DLT. Your idempotency key is `(kafkaPartition, kafkaOffset)`. What goes wrong?**

<details>
<summary>Reveal answer</summary>

Every replayed record is appended to the source topic as a **new** record with a **new** offset — and probably a different partition, since the replay producer runs the partitioner afresh.

So `(partition, offset)` for the replayed copy differs from the original. Your unique constraint sees a brand-new key and inserts a brand-new row.

If any of those 5,000 records had *partially* succeeded before failing — wrote a row, then threw on a downstream call — you now have two rows for one logical event. The idempotency key that protected you against redelivery does not protect you against replay, because it identifies a *physical position in the log*, not a business event.

Replay-safety requires an idempotency key derived from the event's content or a producer-assigned ID: `meta.id` in the Wikimedia payload, an order ID, a request ID. Something stable across republication.

</details>

**4. Two DLT records exist for the same original event, and one has three sets of `kafka_dlt-exception-fqcn` headers. What happened?**

<details>
<summary>Reveal answer</summary>

The record was dead-lettered, replayed, failed again, dead-lettered again, replayed again, and failed a third time.

Headers are an ordered **list**, not a map, and `DeadLetterPublishingRecoverer` *appends* rather than replaces. Each trip through the DLT adds another full set of `kafka_dlt-*` headers to the record it republishes.

So `headers().headers("kafka_dlt-exception-fqcn")` returns three values, oldest first, and `lastHeader()` returns the most recent — the exception from the latest failure. The earlier ones are the failure history, which is genuinely useful: you can see whether the exception type changed after your fix.

The two separate DLT records exist because each replay produced a *new* record to the source topic, and each one failed independently.

</details>

**5. Your DLT consumer logs every failure at `ERROR` with full headers. Is that sufficient production handling?**

<details>
<summary>Reveal answer</summary>

No. It makes failures *legible*, but nothing *notices* them.

A log line is a message to a human who is not reading. Nothing pages, nothing goes red, and consumer lag on the source topic is zero — because dead-lettering a record is, from the source topic's perspective, a successful outcome. Every dashboard stays green while events silently stop reaching your database.

Sufficient handling needs three things the log gives you none of:

- **A metric** — a counter incremented per DLT record, so you can alert on a non-zero *rate*, not a total (a total that stopped growing an hour ago shouldn't page anyone).
- **Durable storage** — a `dead_letter_events` table, so records outlive the DLT's 30-day retention and are queryable without a Kafka consumer.
- **A replay path** — an authenticated endpoint that republishes a record after someone confirms the fix is deployed.

The logging is the easy third of the job, and it's the part that feels finished.

</details>

---

## Recap

Kafka headers are `(String, byte[])`, so the numeric DLT headers are big-endian ints and longs — decode them with `ByteBuffer`, not `new String`, or get control characters and no error. The seven `kafka_dlt-*` headers tell you what failed, why, and exactly where it lives on the source topic, which is what makes fetching the original possible.

A DLT consumer must be the most reliable code you own, because throwing from it creates `.dlt.dlt`. And replay is a repair tool: it needs the fix deployed first, a business-level idempotency key, and an acceptance that ordering is gone.

---

## End of Part 4

Your consumer is now genuinely resilient:

- transient failures retry on a bounded exponential schedule, four attempts over seven seconds
- data-level failures skip retries entirely, because retrying them blocks a partition to no purpose
- an exhausted record is parked on a dead-letter topic, same partition, original bytes, seven diagnostic headers
- the DLT is consumed, decoded, and logged with a full trail back to the source offset

A poison pill no longer stops your pipeline, and no record is silently dropped.

What you still can't do is *use* any of the data you've been storing.

**Next:** [Lesson 23 — A REST API over consumed events →](23-rest-api-over-events.md)
