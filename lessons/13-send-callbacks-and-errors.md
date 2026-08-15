# Lesson 13 — Send Callbacks & Error Handling

> **Part 2 — The Producer** · 20 minutes

---

## What you'll learn

- What `send()` actually returns, and why ignoring it is a bug
- How to read `RecordMetadata` — the partition and offset your record really got
- Which producer exceptions are retriable and which are fatal
- Why blocking on the future destroys your throughput

---

## Why this matters

Since Lesson 08 your producer has logged `"Sent: ..."` immediately after calling `send()`. That line is a lie. It proves the record entered a buffer, nothing more. The broker might have rejected it, the topic might not exist, the batch might have timed out after 120 seconds of retries — and your application would have logged success and moved on.

Every failure mode from the last three lessons — `NotEnoughReplicasException` from `acks=all`, `TimeoutException` from `delivery.timeout.ms`, serialization errors — surfaces in exactly one place: the `CompletableFuture` you have been throwing away.

This lesson picks it up.

---

## Before you start

[Lesson 12](12-batching-linger-compression.md).

---

## The concept

### `send()` returns a future

```java
CompletableFuture<SendResult<K, V>> future = kafkaTemplate.send(topic, key, value);
```

In Spring Kafka 3.x and later this is a real `java.util.concurrent.CompletableFuture`. (Older tutorials show `ListenableFuture`, which was removed — if you see `addCallback(...)`, the code is pre-3.0.)

The future completes when the broker acknowledges the batch containing your record — or completes exceptionally when it doesn't.

### `SendResult` and `RecordMetadata`

On success you get a `SendResult`, whose `getRecordMetadata()` tells you where the record actually landed:

```java
RecordMetadata m = result.getRecordMetadata();
m.topic();        // "wikimedia-stream"
m.partition();    // 1
m.offset();       // 4823
m.timestamp();    // broker or producer timestamp
```

This is the answer to "which partition did my key hash to?" — from the broker, not from your own arithmetic. It's the definitive confirmation the record is durably stored under `acks=all`.

> Notice what this means: the broker was always sending this information back. Lesson 03's console producer simply discarded it. You've been discarding it too.

### Two ways to consume the future

**Non-blocking (correct):**

```java
kafkaTemplate.send(topic, key, value)
        .whenComplete((result, ex) -> { ... });
```

The callback runs on the producer's I/O thread when the acknowledgment arrives. Your calling thread returns immediately. Throughput is preserved.

**Blocking (almost always wrong):**

```java
SendResult<String, String> result = kafkaTemplate.send(topic, key, value).get();
```

`get()` waits for the broker. That means your thread waits at least `linger.ms`, plus the network round trip, plus replication to the ISR — per record. You have just serialised your producer and thrown away batching entirely. A loop of blocking sends is orders of magnitude slower than the same loop non-blocking.

There is one legitimate use: you genuinely cannot proceed until the record is durable — an HTTP handler that must return `201 Created` only if the event was published. Even then, prefer returning the future up the stack.

### Do not swallow the exception

The dangerous shape is this:

```java
.whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Failed to send: {}", ex.getMessage());   // and then... nothing
    }
});
```

The record is gone. It has exhausted `delivery.timeout.ms` worth of retries and will never be sent. You logged a line and continued as though the event had been published. Downstream systems will never see it, and nothing else in your architecture knows.

Logging is not error handling. What you actually owe the caller is one of:

- **propagate** — let the future's failure reach whoever asked you to publish
- **persist** — write the record to an outbox table for a retry job to pick up
- **alert** — increment a failure metric that someone is paged on

Which one depends on whether losing the event is acceptable. What is never acceptable is a lone `log.error`.

### Retriable vs fatal

The Kafka client already retries **retriable** exceptions for you, within `delivery.timeout.ms`. By the time an exception reaches your callback, retrying is over.

| Exception | Retriable? | What it means |
|---|---|---|
| `NotEnoughReplicasException` | yes | ISR below `min.insync.replicas`. Broker recovers, retry succeeds. |
| `NotLeaderOrFollowerException` | yes | Leader election in progress. Client refreshes metadata. |
| `TimeoutException` | yes | `delivery.timeout.ms` expired. Record dropped. |
| `RecordTooLargeException` | **no** | Bigger than `max.request.size`. Retrying changes nothing. |
| `SerializationException` | **no** | Your object can't be serialized. Never fixes itself. |
| `AuthorizationException` | **no** | ACLs. Never fixes itself. |
| `InvalidTopicException` | **no** | Bad topic name. |

The distinction matters because it decides what your callback should *do*. A `TimeoutException` is worth writing to an outbox and retrying later — the cluster may be back in ten seconds. A `SerializationException` will fail identically forever; retrying it is a hot loop, and the only sane response is to drop the record and alert loudly.

You'll meet this same retriable/non-retriable split on the consumer side in Lesson 20, where it decides whether a record goes to the dead-letter topic immediately or after backoff.

---

## Hands-on

### 1. Add the callback

Update `WikimediaProducer`:

```java
package com.javaguy.producer.producer;

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
                    log.debug("Sent → topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                });
    }
}
```

Two details worth noting.

**`log.debug` for success.** At Wikimedia's event rate — often hundreds per second — an `INFO` line per record will drown your logs and become the bottleneck. Success is the boring case.

**An early `return` after the error branch**, rather than `if/else`. Same shape as the house style everywhere else.

### 2. Watch the partitions confirm your hashing

Set the logger to debug in `application.yml`:

```yaml
logging:
  level:
    com.javaguy.producer: DEBUG
```

Run with the keyed sender from Lesson 11:

```
Sent → topic=wikimedia-stream partition=1 offset=0
Sent → topic=wikimedia-stream partition=0 offset=0
Sent → topic=wikimedia-stream partition=1 offset=1
Sent → topic=wikimedia-stream partition=2 offset=0
Sent → topic=wikimedia-stream partition=1 offset=2
Sent → topic=wikimedia-stream partition=0 offset=1
```

Every `en.wikipedia` record reports `partition=1`. Every `de.wikipedia` reports `partition=0`. The broker is confirming, record by record, the affinity you predicted in Lesson 11 — and the offsets within partition 1 count up `0, 1, 2` in send order.

Notice the callbacks arrive **out of send order** across partitions. They fire when each batch is acknowledged, and different partitions have different leaders on different brokers.

### 3. Make it fail

Point the producer at a topic that cannot accept writes, exactly as in Lesson 10:

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --create --topic fail-demo \
  --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=3

docker stop kafka-3
```

Change `TOPIC` to `"fail-demo"` and run. After the retries and `delivery.timeout.ms` elapse:

```
ERROR ... Failed to send key=en.wikipedia: ...NotEnoughReplicasException...
```

Now compare with Lesson 08's producer, which had no callback. **It would have logged `Sent: ...` and exited cleanly.** Same broker state, same lost records, no error anywhere.

Restore:

```bash
docker start kafka-3
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --delete --topic fail-demo
```

### 4. Measure the cost of blocking

Temporarily change `sendMessage` to block:

```java
    public void sendMessage(String key, String message) {
        try {
            var result = kafkaTemplate.send(TOPIC, key, message).get();
            log.debug("offset={}", result.getRecordMetadata().offset());
        } catch (Exception e) {
            throw new IllegalStateException("send failed", e);
        }
    }
```

Send 1,000 records in a loop and time it. Then revert to `whenComplete` and time it again.

Blocking, every record waits for `linger.ms` (20 ms) plus a round trip plus replication — and because the thread is waiting, no further records enter the buffer to batch with. Batches of one, serialised. Non-blocking, the loop fills the buffer in milliseconds and the I/O thread ships large batches.

The gap is typically two to three orders of magnitude. Restore `whenComplete` before moving on.

---

## Try it yourself

1. Produce a record whose value is a 2 MB string. Which exception arrives in the callback, is it retriable, and which setting would you change — on the producer, the topic, or the broker?

2. Add a counter to the error branch and expose it via actuator. Then stop two brokers and produce continuously. What does the counter do, and why is this a better alert than grepping logs for `ERROR`?

3. Set `delivery.timeout.ms: 5000` and `request.timeout.ms: 4000`, stop all brokers, and send one record. Time how long until the callback fires. Now explain what your HTTP handler would have done for those 5 seconds if it had called `.get()`.

---

## Common mistakes

**Ignoring the returned future.**
`send()` returning is not `send()` succeeding. Every producer failure lives in that future.

**Logging the exception and returning.**
The record is permanently lost, and you've recorded that fact in a place nobody reads. Propagate, persist, or alert.

**Calling `.get()` in a loop.**
Serialises the producer, defeats batching and compression, and multiplies latency by the record count.

**Calling `.get()` on a request thread without a timeout.**
`get()` with no argument waits indefinitely — well, until `delivery.timeout.ms`, which is 2 minutes by default. Use `get(timeout, unit)` if you must block.

**Assuming callbacks fire in send order.**
They fire per batch acknowledgment, across partitions with different leaders. Order is only guaranteed within a partition.

**Doing heavy work inside `whenComplete`.**
The callback runs on the producer's single I/O thread. Blocking there stalls *all* sends for *all* partitions. Hand off to an executor.

---

## Check your understanding

**1. Your callback logs `"Failed to send"` and returns. Your service reports 100% uptime and no errors on its dashboards. What has actually happened to those records?**

<details>
<summary>Reveal answer</summary>

They are gone permanently.

By the time the callback's exception fires, the Kafka client has already exhausted its retries within `delivery.timeout.ms`. There is no further attempt, no internal queue, no dead-letter path on the producer side. The record existed only in the producer's memory buffer, and that memory has been reclaimed.

Your dashboards are green because nothing threw — you caught the failure and converted it into a log line. This is why a producer error must increment a metric or persist the record. A log statement is a message to a human who isn't reading.

</details>

**2. `send()` is async. So why does `.get()` in a loop make the producer 100× slower — surely it's the same records over the same network?**

<details>
<summary>Reveal answer</summary>

Because blocking destroys batching, and batching is where the throughput lives.

Non-blocking, the loop dumps 1,000 records into the accumulator in microseconds. The I/O thread groups them into a few large batches and sends a handful of requests.

Blocking, record 1 enters an empty batch. The producer waits `linger.ms` (20 ms) for company that will never arrive, because your only thread is parked in `get()`. It sends a batch of one, waits for the leader to replicate to the ISR, returns — and only then does record 2 exist.

So you pay `linger.ms` + round trip + replication **per record** instead of per batch, and every batch has exactly one record, so compression achieves nothing either. Same records, same network, 1,000 requests instead of 5.

</details>

**3. Why does `RecordMetadata` prove something that your own `murmur2(key) % 3` calculation cannot?**

<details>
<summary>Reveal answer</summary>

Because it comes from the broker, after the record was appended.

Your calculation predicts where the partitioner *should* place the record given a partition count you believe is current. `RecordMetadata` reports where it *was* placed, and — under `acks=all` — that it was replicated to the in-sync replicas before the acknowledgment was sent.

The two can differ. If the topic's partition count changed, if a custom partitioner is configured, or if you passed an explicit partition, your arithmetic is wrong and the metadata is right. The offset in particular is something only the broker can tell you: it's assigned at append time.

</details>

**4. `SerializationException` and `TimeoutException` both arrive in your callback. Why should your code treat them differently?**

<details>
<summary>Reveal answer</summary>

`TimeoutException` is **retriable**: the cluster was unreachable or too slow within the delivery budget, and the very same record may succeed in ten seconds. Writing it to an outbox for a background retry is sensible.

`SerializationException` is **fatal**: the object cannot be turned into bytes by the configured serializer. It will fail identically on every attempt, forever. Retrying it is a hot loop that burns CPU and never converges. The only reasonable actions are to drop the record and alert, or route it somewhere for manual inspection.

Treating both the same means either abandoning recoverable data, or retrying poison data indefinitely.

Note that the client has already retried the retriable ones for you. Anything reaching your callback has finished retrying — you're deciding what to do *after* Kafka gave up.

</details>

**5. A colleague moves the database write inside `whenComplete` so the row is only saved once Kafka confirms. Throughput collapses across every partition. Why?**

<details>
<summary>Reveal answer</summary>

`whenComplete` executes on the producer's **I/O thread** — the single thread responsible for draining the accumulator, sending batches to *every* partition, and processing *every* acknowledgment.

A database write there blocks that thread for milliseconds. While it's blocked, no batches are sent to any partition, no acknowledgments are processed, and the record accumulator backs up. Eventually `buffer.memory` fills and `send()` starts blocking the application threads too, and the whole service stalls.

The callback must do only trivial, non-blocking work: increment a counter, log at debug, complete another future. Anything real belongs on an executor.

(The underlying design — write to Kafka, then write to the DB — also has a correctness problem, since the two aren't atomic. The transactional outbox pattern exists for exactly this.)

</details>

---

## Recap

`send()` returns a `CompletableFuture<SendResult>`. It carries `RecordMetadata` on success — the broker's own statement of topic, partition, and offset — and every producer failure on the other branch. Handle it with `whenComplete` and keep the callback trivial, because it runs on the I/O thread. Don't `.get()` unless you truly must, and never let an error branch end at `log.error`.

Your producer is now durable, deduplicated, keyed, batched, compressed, and honest about failure. It has nothing real to send.

**Next:** [Lesson 14 — Real data: the Wikimedia SSE stream →](14-wikimedia-sse-stream.md)
