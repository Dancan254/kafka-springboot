# Lesson 14 — Real Data: The Wikimedia SSE Stream

> **Part 2 — The Producer** · 30 minutes

---

## What you'll learn

- What Server-Sent Events are, and why they're a natural fit for Kafka
- How to consume an SSE stream with `WebClient` and `Flux`
- Why decoding to `ServerSentEvent<String>` beats parsing the raw body yourself
- Why an unbounded reactive stream feeding a bounded producer buffer is a backpressure problem

---

## Why this matters

Your producer works, but it sends `"edit-1"` at startup. Time to point it at something real: **every edit made to every Wikimedia project, worldwide, as it happens.**

Wikimedia publishes this at `https://stream.wikimedia.org/v2/stream/recentchange` as a public, unauthenticated Server-Sent Events feed. It emits somewhere between tens and hundreds of events per second, forever. It is an ideal Kafka source: a firehose you don't control, that you want to buffer, replay, and fan out to many consumers.

This is also the lesson where every setting from Lessons 10–13 stops being theoretical. Real volume, real batches, real compression.

---

## Before you start

[Lesson 13](13-send-callbacks-and-errors.md). Working internet connection.

Watch the raw feed before you write any code — always look at the data first:

```bash
curl -N https://stream.wikimedia.org/v2/stream/recentchange | head -20
```

Ctrl-C when you've seen enough.

---

## The concept

### Server-Sent Events

SSE is a one-directional streaming protocol over ordinary HTTP. The server holds the response open and pushes text frames. Each frame looks like:

```
event: message
id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":5410942}]
data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{...},"type":"edit","title":"Nikola Tesla",...}

```

Fields are `event:`, `id:`, `data:`, `retry:`. A blank line terminates the frame. The payload you want is the JSON after `data:`.

Compared to WebSockets, SSE is server-to-client only, runs over plain HTTP, and reconnects automatically. For a public event feed it's exactly right.

> Look at that `id:` field. Wikimedia's own feed is backed by Kafka, and the SSE `id` is a Kafka topic/partition/offset. You are consuming someone else's Kafka topic over HTTP, and about to put it into yours.

### Why not just read the body?

You could stream the raw response and split on newlines yourself. Don't. You'd have to handle:

- multi-line `data:` fields (concatenated with `\n`)
- comment lines beginning with `:` (used as keep-alives)
- blank-line frame boundaries
- `retry:` directives and reconnection

Spring's `WebClient` already decodes all of this. Ask for `ServerSentEvent<String>` and you get parsed frames, with `.data()` giving you the payload and nothing else.

```java
.bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
```

The `ParameterizedTypeReference` exists because of generic type erasure — `ServerSentEvent<String>.class` isn't expressible in Java, so this anonymous subclass carries the type at runtime. It's the same trick as Jackson's `TypeReference`.

If you instead call `.bodyToFlux(String.class)`, WebClient still decodes SSE frames and hands you the `data:` payloads — but you lose access to `event`, `id`, and `retry`. Decoding to `ServerSentEvent<String>` keeps them.

### `Flux` and the shape of the pipeline

`bodyToFlux` returns a `Flux<ServerSentEvent<String>>` — a reactive stream of zero-to-infinite elements. Nothing happens until you **subscribe**; a `Flux` is a recipe, not a running process.

```java
webClient.get()
        .uri("/stream/recentchange")
        .retrieve()
        .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
        .filter(event -> event.data() != null && !event.data().isBlank())
        .subscribe(
                event -> producer.sendMessage(event.data()),
                error -> log.error("SSE stream error: {}", error.getMessage())
        );
```

`subscribe()` returns immediately and the stream runs on a Reactor Netty event-loop thread. The `filter` guards against keep-alive frames, which arrive with a null or empty `data`.

### The backpressure problem hiding in plain sight

This is the important part, and most tutorials skip it.

`subscribe(consumer)` with a plain lambda requests an **unbounded** number of elements. Reactor will push events at you as fast as Wikimedia sends them.

Meanwhile `producer.sendMessage()` appends to a bounded 32 MiB buffer (Lesson 12). If Kafka slows down and the buffer fills, `send()` **blocks** for `max.block.ms`.

That block happens on a Netty event-loop thread — a thread shared with other network I/O, which you must never block. In a small demo the event rate is modest and Kafka is local, so it never bites. Under a real load spike, or a broker failover, it would.

The correct answers are `onBackpressureBuffer` with a bounded queue and an explicit overflow strategy, or `limitRate()` to bound the demand, or publishing onto a dedicated scheduler with `publishOn(Schedulers.boundedElastic())`. The version below keeps the project's original shape so it matches the reference implementation; the exercise asks you to fix it.

---

## Hands-on

### 1. Add WebFlux

`WebClient` lives in WebFlux. Add it to `pom.xml`:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
</dependency>
```

This also starts a Netty web server, which is what will serve the controller in step 4.

### 2. `config/WebClientConfig.java`

```java
package com.javaguy.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient.Builder webClientBuilder() {
        return WebClient.builder();
    }
}
```

Exposing the *builder* rather than a finished `WebClient` lets each consumer configure its own base URL, while still picking up Boot's auto-configured codecs and observability instrumentation.

### 3. `stream/WikimediaStreamConsumer.java`

The name is unfortunate but it's the project's: this class *consumes* the SSE stream and *produces* to Kafka. It has nothing to do with a Kafka consumer.

```java
package com.javaguy.producer.stream;

import com.javaguy.producer.producer.WikimediaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class WikimediaStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaStreamConsumer.class);

    private final WebClient webClient;
    private final WikimediaProducer producer;

    public WikimediaStreamConsumer(WebClient.Builder webClientBuilder, WikimediaProducer producer) {
        this.webClient = webClientBuilder.baseUrl("https://stream.wikimedia.org/v2").build();
        this.producer = producer;
    }

    public void consumeStreamAndPublish() {
        // Decode SSE framing so only the JSON payload reaches Kafka —
        // no "data:" prefixes, no keep-alive comments.
        webClient.get()
                .uri("/stream/recentchange")
                .retrieve()
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .filter(event -> event.data() != null && !event.data().isBlank())
                .subscribe(
                        event -> producer.sendMessage(event.data()),
                        error -> log.error("SSE stream error: {}", error.getMessage())
                );
    }
}
```

### 4. `controller/WikimediaController.java`

```java
package com.javaguy.producer.controller;

import com.javaguy.producer.stream.WikimediaStreamConsumer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/wikimedia")
public class WikimediaController {

    private final WikimediaStreamConsumer wikimediaStreamConsumer;

    public WikimediaController(WikimediaStreamConsumer wikimediaStreamConsumer) {
        this.wikimediaStreamConsumer = wikimediaStreamConsumer;
    }

    @GetMapping
    public ResponseEntity<String> startPublishing() {
        wikimediaStreamConsumer.consumeStreamAndPublish();
        return ResponseEntity.accepted().body("Streaming started");
    }
}
```

`202 Accepted` is the honest status code: you've started a long-running background process, not completed a request. Returning `ResponseEntity` with an explicit status beats a `void` method that silently 200s.

### 5. Update the producer for raw JSON

The SSE payload is a JSON document. For now, send it unkeyed — matching the reference implementation, and matching the discussion in Lesson 11 about this project not needing per-record ordering.

Simplify `sendMessage` back to a single argument:

```java
    public void sendMessage(String message) {
        kafkaTemplate.send(TOPIC, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message: {}", ex.getMessage());
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.debug("Sent → topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                });
    }
```

### 6. Delete the startup sender

`StartupMessageSender` has served its purpose. Delete the file. Real events now come from the stream, triggered by an HTTP call.

### 7. Run it

```bash
./mvnw spring-boot:run
```

The app starts on port 8081 and does nothing. The stream is not running yet — `consumeStreamAndPublish()` hasn't been called.

Trigger it:

```bash
curl http://localhost:8081/api/v1/wikimedia
```

```
Streaming started
```

Watch the topic fill:

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

Run it twice, a few seconds apart. The end offsets climb. That's live Wikipedia.

Look at a record:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream --max-messages 1
```

A fat JSON document with `type`, `title`, `user`, `bot`, `wiki`, `server_name`, `timestamp`, `comment`, and much more. In Lesson 16 you'll map exactly the fields you need and ignore the rest.

### 8. Now check your batching

With real volume, the metrics from Lesson 12 finally mean something:

```bash
curl -s localhost:8081/actuator/metrics/kafka.producer.batch.size.avg
curl -s localhost:8081/actuator/metrics/kafka.producer.compression.rate.avg
```

*(Add `spring-boot-starter-actuator` and expose `metrics` if you haven't.)*

`compression.rate.avg` should now be well below 1.0 — these JSON documents share enormous structure, and `snappy` over a 32 KiB batch of them is very effective. With the startup sender's six records, this metric was meaningless. With hundreds per second, `linger.ms=20` fills real batches.

Everything you configured in Lesson 12 was invisible until now.

---

## Try it yourself

1. Call `GET /api/v1/wikimedia` **twice**. Look at the produce rate. What did you just do, and what would a third call do? What's the fix — and why is a `@GetMapping` that mutates server state the wrong verb anyway?

2. Fix the backpressure hole. Insert `.limitRate(256)` before `.subscribe(...)`, or `.onBackpressureBuffer(10_000)` with an overflow strategy. Then stop all three brokers while the stream runs and observe what happens to the Netty event loop with and without your fix.

3. Key the records by page title. You don't have parsed JSON yet, but `ServerSentEvent` gives you `.id()` — Wikimedia's own Kafka offset. Is that a good key? (Think cardinality and ordering: what would it group?)

4. The `subscribe(...)` error handler logs and the stream **terminates**. Wikimedia will drop a long-lived connection eventually. Add `.retryWhen(Retry.backoff(...))` and confirm the stream recovers.

---

## Common mistakes

**Calling `bodyToFlux(String.class)` and then string-splitting on `data:`.**
WebClient already decodes SSE. Decode to `ServerSentEvent<String>` and read `.data()`.

**Forgetting that a `Flux` does nothing until subscribed.**
No `subscribe()`, no stream. Building the pipeline in a `@PostConstruct` and never subscribing is a silent no-op.

**Calling the trigger endpoint more than once.**
Each call opens another SSE connection and another subscription, publishing every event twice, three times, `n` times. Nothing prevents it.

**Blocking a Netty event-loop thread.**
`producer.sendMessage()` blocks once the record accumulator fills. On an event loop, that stalls unrelated I/O. Use `publishOn(Schedulers.boundedElastic())` or bound demand.

**Letting the stream die silently.**
The error consumer in `subscribe()` is terminal — it logs, and the `Flux` is finished. Without `retryWhen`, one network blip ends your ingestion until someone re-calls the endpoint.

**Using `@GetMapping` to start a background job.**
`GET` should be safe and idempotent. Crawlers, browser prefetch, and monitoring probes will all hit it.

---

## Check your understanding

**1. You call `GET /api/v1/wikimedia` three times. What happens to your topic?**

<details>
<summary>Reveal answer</summary>

Every event is produced three times.

Each call runs `consumeStreamAndPublish()` again, which opens a **new** HTTP connection to Wikimedia and creates a **new** subscription. Three independent `Flux` pipelines now push the same events into `sendMessage()`.

Nothing deduplicates them. Idempotence (Lesson 10) does not help: these are three genuinely distinct `send()` calls with three different sequence numbers, not internal retries of one. Kafka stores all three faithfully.

Guarding against this needs an `AtomicBoolean` (or a `Disposable` you check and cancel), and the endpoint should be a `POST` — starting a background stream is neither safe nor idempotent.

</details>

**2. Why does `bodyToFlux` need `new ParameterizedTypeReference<ServerSentEvent<String>>() {}` rather than `ServerSentEvent.class`?**

<details>
<summary>Reveal answer</summary>

Java erases generics at runtime, so there is no `Class` object representing `ServerSentEvent<String>` — only `ServerSentEvent`. Passing the raw class would leave WebClient unable to know how to decode the `data:` payload.

`ParameterizedTypeReference` is an anonymous subclass whose *supertype* carries the full generic signature. Because a class's generic superclass **is** retained in the bytecode, WebClient can reflect on it and recover `ServerSentEvent<String>` at runtime.

That's why it's always written with trailing `{}` — the braces create the subclass. Without them it wouldn't compile. Jackson's `TypeReference` uses the identical trick.

</details>

**3. `compression.rate.avg` was ~0.98 in Lesson 12 and is now ~0.25, with no configuration change. What changed?**

<details>
<summary>Reveal answer</summary>

The event rate.

Compression works on batches. Previously the producer sent six records at startup, so `linger.ms=20` expired with a single record in each batch, and compressing one 200-byte document achieves nothing.

Now hundreds of records per second arrive. Batches fill toward `batch.size` (32 KiB) before the linger timer expires, and each batch holds many Wikimedia JSON documents that share nearly identical field names and structure. Snappy exploits that repetition heavily.

The lesson: compression ratio is a function of your *throughput and batching*, not just your codec. You can't evaluate `compression.type` on a test that sends ten records.

</details>

**4. `subscribe()` returns immediately, on the calling HTTP thread. On which thread does `producer.sendMessage()` run, and why is that a problem?**

<details>
<summary>Reveal answer</summary>

On a Reactor Netty **event-loop thread** — one of a small, fixed pool (typically one per CPU core) that handles all non-blocking network I/O for the application.

`sendMessage()` calls `kafkaTemplate.send()`, which is non-blocking *only while the record accumulator has room*. If Kafka slows — a leader election, a broker failover, `min.insync.replicas` unsatisfied — the 32 MiB buffer fills and `send()` blocks for up to `max.block.ms` (60 s).

Blocking an event-loop thread stalls every other connection that loop is multiplexing, including the SSE stream itself and any HTTP requests your service is serving. A Kafka slowdown becomes a total application stall.

The fix is to move the blocking work off the loop (`publishOn(Schedulers.boundedElastic())`), bound the demand (`limitRate`), or bound the buffer explicitly (`onBackpressureBuffer` with a drop or error strategy).

</details>

**5. The SSE `id:` field contains Wikimedia's own Kafka topic, partition, and offset. What does that tell you, and why would it make a poor partition key for your topic?**

<details>
<summary>Reveal answer</summary>

It tells you Wikimedia's `recentchange` feed *is* a Kafka topic, exposed over HTTP. You're building a bridge from someone else's Kafka to your own, and the `id` is the resume token you'd use to reconnect without gaps.

As a partition key it would be terrible: the offset increments on every single event, so the key is unique per record. That gives perfect distribution and **zero** ordering benefit — every record is its own group of one. You'd pay to hash a value that groups nothing, and fragment your batches into the bargain.

A good key groups records that must stay ordered relative to each other. The page `title` does that. A monotonically increasing offset does the opposite of that.

</details>

---

## Recap

You pointed the producer at a live firehose. `WebClient` decodes SSE framing into `ServerSentEvent<String>`, a `Flux` streams the frames, and each `data:` payload becomes a Kafka record. Real volume finally made `linger.ms`, `batch.size`, and `compression.type` do visible work.

You also inherited two real weaknesses from the reference implementation: the endpoint can be triggered repeatedly, and the reactive stream pushes unboundedly into a bounded, blocking producer buffer on an event-loop thread. Both are in the exercises, and both are the kind of thing that only fails in production.

---

## End of Part 2

Your producer is complete. It:

- uses `spring-boot-starter-kafka`, so `KafkaTemplate` actually exists
- declares its topic in code with the durability settings from Lesson 06
- writes with `acks=all` and idempotence, so a lost acknowledgment can't duplicate a record
- can key records to control partition affinity, and you know what that costs
- batches, lingers, and compresses — and you've seen the metrics prove it
- reports every failure through a callback rather than pretending success
- streams live Wikimedia edits

The finished producer lives at [`producer/`](../producer) in the repository root, if you want to diff your work against it.

Nothing is reading any of it. Time to build the other side.

**Next:** [Lesson 15 — Your first `@KafkaListener` →](15-first-kafkalistener.md)
