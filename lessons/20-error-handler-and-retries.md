# Lesson 20 — `DefaultErrorHandler` & Retries

> **Part 4 — Resilience** · 30 minutes

---

## What you'll learn

- What Spring's `DefaultErrorHandler` does when your listener throws
- How `ExponentialBackOff` produces a bounded retry schedule, and how `maxElapsedTime` ends it
- Why retrying a malformed record is not just useless but harmful
- How to classify exceptions as retryable or fatal, and why the classification is a design decision

---

## Why this matters

In Lesson 16 you produced `this is not json` to the topic and watched the consumer retry it forever, ten times a second, blocking the partition. Every valid record behind it waited. Your only escape was resetting the group's offsets and skipping *everything* pending.

That's the poison-pill problem, and it is the single most common way a Kafka consumer goes down in production. The record is not corrupt — Kafka stored exactly what the producer sent. Your code simply cannot process it, and will never be able to.

This lesson gives you the retry policy. The next one gives you somewhere to put the records that survive it.

---

## Before you start

[Lesson 19](19-concurrency-and-rebalancing.md). A consumer with `setConcurrency(3)`, manual ack, and JPA persistence.

---

## The concept

### What happens when your listener throws

The listener container catches the exception and hands the record, plus the exception, to a **`CommonErrorHandler`**.

If you configure nothing, you get `DefaultErrorHandler` with default settings: retry the record **9 times with no delay**, then log the failure and skip it. That's the ten-per-second hammering you saw — nine near-instant retries, then it gives up on *that* delivery, but because the offset was never committed, the next `poll()` returns the same record and the cycle begins again.

Two things worth being precise about:

**Retries are seeks, not redeliveries.** The error handler tells the consumer to `seek()` back to the failed record's offset. The next poll returns it again. There's no separate retry queue and no broker involvement.

**Retries block the partition.** The consumer thread is busy re-processing one record. Everything behind it on that partition waits. With `setConcurrency(3)`, the other two partitions keep flowing — so a poison pill costs you a third of your throughput, not all of it.

### `DefaultErrorHandler`, properly configured

```java
var handler = new DefaultErrorHandler(recoverer, backOff);
```

Two collaborators:

- a **`BackOff`** — how long to wait between attempts, and when to stop
- a **recoverer** — a `BiConsumer<ConsumerRecord, Exception>` invoked once retries are exhausted

Without a recoverer, an exhausted record is logged and skipped. With one — Lesson 21's `DeadLetterPublishingRecoverer` — it's published somewhere you can inspect it.

> `DefaultErrorHandler` replaced `SeekToCurrentErrorHandler`, which was removed in Spring Kafka 3.x. If a tutorial mentions `SeekToCurrentErrorHandler` or `ErrorHandler`/`BatchErrorHandler`, it predates 2022.

### `ExponentialBackOff` and the `maxElapsedTime` trick

```java
var backoff = new ExponentialBackOff(1_000L, 2.0);
backoff.setMaxInterval(10_000L);
backoff.setMaxElapsedTime(7_000L);
```

`ExponentialBackOff(initialInterval, multiplier)` produces intervals `1s, 2s, 4s, 8s, …`, capped at `maxInterval`.

`maxElapsedTime` is what stops it — and it works in a way that surprises people. The backoff tracks the **cumulative sum of intervals it has handed out**, not real wall-clock time. Each call to `nextBackOff()` adds to that total. Once the total reaches `maxElapsedTime`, the next call returns `BackOffExecution.STOP`, and the error handler invokes the recoverer.

So `maxElapsedTime = 7_000` gives you exactly:

| Attempt | Wait before it | Cumulative |
|---|---|---|
| 1 | — (immediate) | 0 |
| 2 | 1,000 ms | 1,000 |
| 3 | 2,000 ms | 3,000 |
| 4 | 4,000 ms | 7,000 |
| — | `STOP` → recoverer | |

Four attempts, three retries, `1 + 2 + 4 = 7` seconds of waiting. Change `maxElapsedTime` to `15_000` and you'd get a fifth attempt after another 8 s.

> The alternative is `ExponentialBackOffWithMaxRetries(3)`, which counts attempts directly and is easier to read. This project uses the `maxElapsedTime` form; both are correct, and knowing that `maxElapsedTime` sums *intervals* rather than measuring elapsed time is the part that catches people out. If your listener takes 30 seconds per attempt, the backoff neither knows nor cares.

### Retryable vs non-retryable

Here's the crux.

A retry is a bet that **the same input will produce a different outcome next time**. That bet pays off when the failure was caused by something transient and external:

- the database was briefly down
- a downstream HTTP call timed out
- the ISR dipped below `min.insync.replicas`

It never pays off when the failure is caused by the *record itself*:

- the JSON is malformed
- a required field is missing
- the value fails a business validation rule

The bytes don't change between attempts. Retrying a malformed record four times, waiting seven seconds in the process, blocks the partition for seven seconds and then fails anyway. Retrying it *forever* blocks the partition forever.

```java
handler.addNotRetryableExceptions(IllegalArgumentException.class);
```

This tells the handler: when you see this exception type, skip the backoff entirely and go straight to the recoverer.

Now look back at Lesson 16's parse method:

```java
} catch (JacksonException e) {
    throw new IllegalArgumentException("Unparseable Wikimedia event ...", e);
}
```

That wrapping was never cosmetic. It was choosing the exception type that this line will match. `JacksonException` would have been retried; `IllegalArgumentException` is not. **The exception type is the routing decision**, made three lessons before the router existed.

The classification is a design choice, and it's yours. Kafka has no opinion about whether your exception is worth retrying. Choose deliberately:

| Exception | Retryable? | Because |
|---|---|---|
| `DataAccessException` | yes | the database may come back |
| `RestClientException` | yes | the downstream service may recover |
| `IllegalArgumentException` | **no** | the record is invalid and always will be |
| `NullPointerException` | **no** | that's a bug in your code; retrying hides it |

`addNotRetryableExceptions` matches on assignability, so registering `IllegalArgumentException` also catches `NumberFormatException`, which extends it.

---

## Hands-on

### 1. Reproduce the poison pill

Start your consumer, then produce garbage:

```bash
echo 'this is not json' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

The consumer floods:

```
ERROR ... Unparseable Wikimedia event [partition=1 offset=8423]: Unrecognized token 'this'
ERROR ... Unparseable Wikimedia event [partition=1 offset=8423]: Unrecognized token 'this'
ERROR ... Unparseable Wikimedia event [partition=1 offset=8423]: Unrecognized token 'this'
```

Same partition, same offset, forever. Confirm the partition is stuck:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group wikimedia-consumer-group
```

One partition's `CURRENT-OFFSET` is frozen while its `LAG` grows. The other two are fine.

Stop the consumer.

### 2. Add the error handler

Update `KafkaConsumerConfig`:

```java
package com.javaguy.consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Retry schedule, per record:
     *   attempt 1 — immediate
     *   attempt 2 — after 1,000 ms
     *   attempt 3 — after 2,000 ms
     *   attempt 4 — after 4,000 ms, then give up
     *
     * ExponentialBackOff sums the intervals it hands out. Once that total reaches
     * maxElapsedTime (1,000 + 2,000 + 4,000 = 7,000), the next call returns STOP.
     * It is not measuring wall-clock time, so a slow listener does not shorten it.
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        var backoff = new ExponentialBackOff(1_000L, 2.0);
        backoff.setMaxInterval(10_000L);
        backoff.setMaxElapsedTime(7_000L);

        var handler = new DefaultErrorHandler(backoff);

        // A malformed record will never parse, no matter how often it is retried.
        // Skip the backoff entirely rather than block the partition for 7 seconds.
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

No recoverer yet, so an exhausted record is logged and **skipped**. That's better than an infinite loop and worse than what you want — the record is silently discarded. Lesson 21 fixes that.

### 3. Watch a non-retryable exception skip the backoff

Restart the consumer. The poison record from step 1 is still there, still uncommitted.

```
ERROR ... Unparseable Wikimedia event [partition=1 offset=8423]: Unrecognized token 'this'
ERROR ... Backoff none exhausted for wikimedia-stream-1@8423
INFO  ... Saved | partition=1 offset=8424 ...
```

**One attempt.** No 1-second wait, no 2-second wait. `Backoff none exhausted` is Spring telling you it applied a zero-length backoff because the exception was classified non-retryable. The record is skipped, the offset advances, and the partition unblocks immediately.

The pipeline resumes. Records behind the poison pill flow again.

### 4. Watch a retryable exception use the full schedule

Now simulate a transient failure. Temporarily make every fifth record throw something *not* on the non-retryable list:

```java
    if (record.offset() % 5 == 0) {
        throw new IllegalStateException("simulated transient failure");
    }
    repository.save(event);
```

Restart and watch the timestamps:

```
11:42:01.204 ERROR ... simulated transient failure
11:42:02.213 ERROR ... simulated transient failure
11:42:04.219 ERROR ... simulated transient failure
11:42:08.226 ERROR ... simulated transient failure
11:42:08.230 ERROR ... Backoff FixedBackOff/ExponentialBackOff exhausted for wikimedia-stream-0@8500
```

Four attempts. The gaps are 1 s, 2 s, 4 s — exactly the schedule from the table. Then the handler gives up and skips.

Note the elapsed time between the first and last attempt: about 7 seconds, during which **that partition processed nothing else.** Retrying is not free. It is a deliberate trade of latency for the chance that the failure was transient.

Remove the simulated failure.

### 5. See why the classification matters

Swap the classification — register `IllegalStateException` as non-retryable and leave `IllegalArgumentException` retryable:

```java
handler.addNotRetryableExceptions(IllegalStateException.class);
```

Produce another malformed record. Now the parse failure gets the full 7-second backoff before being skipped, and the transient failure gets none.

You have exactly inverted the correct behaviour, and nothing in Kafka or Spring will tell you. The record that could never succeed is retried; the record that might succeed on the next attempt is not.

Put it back to `IllegalArgumentException`.

---

## Try it yourself

1. Set `backoff.setMaxElapsedTime(15_000L)`. How many attempts now, and what are the intervals? Verify against the log timestamps rather than the table.

2. Replace `ExponentialBackOff` with `new ExponentialBackOffWithMaxRetries(3)` and set the multiplier and initial interval to match. Same behaviour? Which version would you rather read in a code review six months from now?

3. Make `repository.save()` throw `DataIntegrityViolationException` (produce the same record twice with a unique constraint). Is that exception retryable by default? Should it be? Note the trap: it's a `DataAccessException`, and the *same* record will fail identically forever.

4. With `setConcurrency(3)`, produce a poison pill and measure the throughput drop with the old (infinite retry) handler versus the new one. Why is it exactly one third and not total?

---

## Common mistakes

**Configuring no error handler at all.**
You get 9 instant retries, then a skip — but since the offset was never committed, the record is redelivered on the next poll and the loop restarts. Effectively infinite.

**Retrying every exception.**
A malformed record blocks its partition for the whole backoff window, every single delivery, and is then discarded anyway. You paid the latency for nothing.

**Retrying `NullPointerException`.**
It's a bug in your code. Retrying it four times just delays the moment you notice.

**Assuming `maxElapsedTime` is wall-clock time.**
It's the sum of the backoff intervals handed out. A listener that takes 30 s per attempt doesn't consume any of the budget.

**Configuring the error handler and forgetting `factory.setCommonErrorHandler(...)`.**
The bean exists and is never used. Spring's default handler stays in place, silently.

**Using `SeekToCurrentErrorHandler`.**
Removed in Spring Kafka 3.x.

**Exhausting retries with no recoverer.**
The record is logged and skipped — permanently lost. That's the state your consumer is in right now.

---

## Check your understanding

**1. Your listener throws on record at offset 500. The error handler retries it. Where does the record come from on the second attempt?**

<details>
<summary>Reveal answer</summary>

From the broker, on the next `poll()`.

`DefaultErrorHandler` doesn't hold the record in memory and re-invoke your method. It calls `seek()` on the consumer to reposition it back to offset 500, so the next poll fetches that record again — along with the ones after it — and the container invokes your listener with it.

Two consequences follow. First, retrying is genuinely re-consuming, so the whole partition is rewound and everything after offset 500 is re-fetched. Second, because the offset was never committed, a consumer restart mid-retry begins the retry sequence again from scratch, with a fresh backoff.

</details>

**2. `ExponentialBackOff(1_000L, 2.0)` with `maxElapsedTime(7_000L)`. Your listener takes 30 seconds per attempt. How many attempts, and how long does the whole thing take?**

<details>
<summary>Reveal answer</summary>

Still **four attempts** — and about 127 seconds.

`maxElapsedTime` bounds the sum of the *backoff intervals* the strategy has handed out (0 + 1,000 + 2,000 + 4,000 = 7,000), not the real time spent. Time your listener spends executing is invisible to the backoff.

So you get: 30 s attempt, 1 s wait, 30 s attempt, 2 s wait, 30 s attempt, 4 s wait, 30 s attempt, then STOP. That's 120 s of processing plus 7 s of waiting.

Which also means you've blocked that partition for over two minutes — and quite possibly tripped `max.poll.interval.ms` (5 min) along the way if the batch had more failing records.

</details>

**3. Why is retrying a malformed JSON record worse than simply skipping it immediately?**

<details>
<summary>Reveal answer</summary>

Because the retry cannot possibly succeed, and it costs you the partition for the duration.

The record's bytes are fixed. `objectMapper.readValue()` is deterministic. Attempt four will fail exactly as attempt one did. Meanwhile the consumer thread is seeking back and re-polling the same offset, so every valid record behind it on that partition is delayed by the full 7-second backoff window.

Worse, this happens on *every delivery*. If the record is never committed or routed away, that's 7 seconds of blockage each time it comes round.

Skipping immediately is better. Routing it to a dead-letter topic — where you can inspect it and fix the producer — is better still, and that's Lesson 21.

</details>

**4. You register `IllegalArgumentException` as non-retryable. Your code throws `NumberFormatException`. Is it retried?**

<details>
<summary>Reveal answer</summary>

No — it's treated as non-retryable.

`addNotRetryableExceptions` matches on **assignability**, not exact type equality. `NumberFormatException extends IllegalArgumentException`, so it matches the registered type and skips the backoff.

This is usually what you want, and it's occasionally a surprise: registering a broad exception type silently classifies its entire subtree. Registering `RuntimeException` as non-retryable would make *everything* non-retryable, since almost every exception you throw extends it.

The same assignability rule applies to the retryable side of the classifier.

</details>

**5. The error handler exhausts its retries and no recoverer is configured. What happens to the record, and what does your monitoring see?**

<details>
<summary>Reveal answer</summary>

The record is logged at `ERROR` and **skipped**. The offset advances past it, the partition unblocks, and the record is never processed.

It is not deleted from Kafka — it sits in the topic until retention expires — but nothing in your system will ever look at it again. From your database's perspective, that event simply never happened.

Your monitoring sees: consumer lag returns to zero, no failed health check, no rising error rate beyond a single log line per lost record. Everything looks healthy. You have silent data loss that presents identically to normal operation.

This is precisely why the recoverer exists. A dead-letter topic converts "lost" into "parked somewhere I can find it."

</details>

---

## Recap

`DefaultErrorHandler` catches your listener's exception, seeks back, and retries on a `BackOff` schedule. `ExponentialBackOff` with `maxElapsedTime(7_000)` yields four attempts at 1 s, 2 s, 4 s — because it sums the intervals it issues, not wall-clock time. Exceptions you classify as non-retryable skip the backoff entirely, which is why Lesson 16 wrapped `JacksonException` in `IllegalArgumentException` three lessons before this handler existed.

Your partition no longer blocks. But an exhausted record is now silently discarded, which is its own kind of data loss.

**Next:** [Lesson 21 — Dead-letter topics →](21-dead-letter-topics.md)
