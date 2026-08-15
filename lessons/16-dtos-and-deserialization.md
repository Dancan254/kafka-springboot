# Lesson 16 — DTO Records & Deserialization

> **Part 3 — The Consumer** · 20 minutes

---

## What you'll learn

- Why a Java `record` is the right shape for an inbound event
- How `@JsonIgnoreProperties(ignoreUnknown = true)` protects you from the producer's schema changes
- Why Spring Boot 4 ships Jackson 3, and what that changes
- Why parsing *inside* the listener beats configuring a `JsonDeserializer`

---

## Why this matters

The consumer currently logs `valueLength=1834`. Inside that string is a Wikimedia event with about forty fields, of which you want nine.

How you turn that string into an object determines what happens when the string is malformed — and malformed input is guaranteed, eventually. The choice you make here decides whether a single bad record kills your consumer, blocks a partition forever, or gets quietly diverted for inspection.

That decision is the seed of Part 4.

---

## Before you start

[Lesson 15](15-first-kafkalistener.md). A consumer that logs records.

Look at the actual payload first:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream --max-messages 1
```

A large JSON document: `$schema`, `meta`, `id`, `type`, `namespace`, `title`, `comment`, `timestamp`, `user`, `bot`, `server_name`, `wiki`, and more.

---

## The concept

### Take only what you need

You could map all forty fields. Don't.

Every field you bind is a field you have coupled yourself to. Wikimedia can add, rename, or remove fields at any time — you don't control their schema. Bind nine, ignore the rest, and their changes cannot break you unless they touch the nine.

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikimediaEventDto(...) {}
```

Without `ignoreUnknown = true`, Jackson throws `UnrecognizedPropertyException` on the first unmapped field. With it, unmapped fields are silently discarded. For a payload you don't own, this is not laziness — it's the only sane default.

### Records, not classes

An inbound event is immutable data. It has no behaviour, no lifecycle, no identity. That is precisely what a `record` is for:

```java
public record WikimediaEventDto(String type, String title, ...) {}
```

You get a constructor, accessors, `equals`, `hashCode`, and `toString` — and, more importantly, you cannot accidentally mutate an event halfway through processing.

Jackson binds to records natively. It uses the canonical constructor, so there's no no-args constructor and no setters to expose.

> This is also why the house convention is **no Lombok on DTOs**. `@Data` on a class gives you a mutable object with setters, which is the opposite of what an event should be. Records make Lombok unnecessary here.

### Field names that don't match

Wikimedia sends `server_name`. Java wants `serverName`.

```java
@JsonProperty("server_name") String serverName
```

You could instead configure a global `SNAKE_CASE` naming strategy, but that applies to every type in the application and couples your whole codebase to one external system's convention. An annotation on the one field that differs is more honest.

### Jackson 3 — a Spring Boot 4 surprise

Spring Boot 4 ships **Jackson 3**, and Jackson 3 changed its package names:

| | Jackson 2 | Jackson 3 |
|---|---|---|
| `ObjectMapper` | `com.fasterxml.jackson.databind` | **`tools.jackson.databind`** |
| Core exception | `com.fasterxml.jackson.core.JsonProcessingException` | **`tools.jackson.core.JacksonException`** |
| Annotations | `com.fasterxml.jackson.annotation` | **unchanged** |

Two things follow.

**The annotations still import from `com.fasterxml.jackson.annotation`.** `@JsonProperty` and `@JsonIgnoreProperties` live in `jackson-annotations`, which kept its coordinates. So a DTO written for Jackson 2 needs no changes.

**`JsonProcessingException` is gone.** In Jackson 3, `JacksonException` extends `RuntimeException` — parsing failures are *unchecked*. Code that used to `catch (JsonProcessingException e)` no longer compiles, and code that didn't catch anything now compiles but throws at runtime.

You also need the dependency explicitly. In Spring Boot 4, Jackson is **not** transitively pulled in by the web starter:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-jackson</artifactId>
</dependency>
```

This is what provides the auto-configured `ObjectMapper` bean you'll inject.

### Where to deserialize: two options

**Option A — configure Kafka's `JsonDeserializer`:**

```yaml
value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
```

The record arrives as a `WikimediaEventDto` directly. Clean, and wrong for our purposes.

Deserialization now happens *before* your listener is invoked — inside the Kafka client. A malformed record throws a `SerializationException` that your listener never sees, and which the default error handler cannot route anywhere sensible. Handling it needs an `ErrorHandlingDeserializer` wrapper, which adds a layer of indirection precisely where you want clarity.

Worse, the record's raw bytes are gone. When you later want to inspect the bad payload, you have a stack trace and no data.

**Option B — take the `String`, parse it yourself:**

```java
public void consume(ConsumerRecord<String, String> record) {
    WikimediaEventDto dto = parse(record);
    ...
}
```

Deserialization is now ordinary application code inside your listener. You can catch the failure, wrap it in an exception type of your choosing, log the raw payload, and decide exactly what happens next.

That last point is the whole reason. In Lesson 20 you'll configure an error handler that treats some exceptions as retryable and some as fatal. A malformed JSON document will *never* parse, no matter how many times you retry it — so it should skip retries entirely and go straight to the dead-letter topic.

To express that, you need to control the exception type. Option A doesn't let you.

---

## Hands-on

### 1. Add Jackson

`pom.xml`:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-jackson</artifactId>
</dependency>
```

### 2. `dto/WikimediaEventDto.java`

```java
package com.javaguy.consumer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Maps only the fields we care about. Unrecognised fields are discarded, so
 * Wikimedia can evolve its payload without breaking this consumer.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikimediaEventDto(
        String type,
        String title,
        String user,
        boolean bot,
        Integer namespace,
        String wiki,
        @JsonProperty("server_name") String serverName,
        Long timestamp,
        String comment
) {}
```

Nine fields out of forty. `namespace` and `timestamp` are boxed because Wikimedia may omit them; `bot` is a primitive because it's always present.

### 3. Parse inside the listener

```java
package com.javaguy.consumer.consumer;

import com.javaguy.consumer.dto.WikimediaEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
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
    public void consume(ConsumerRecord<String, String> record) {
        WikimediaEventDto dto = parse(record);

        log.info("Consumed | partition={} offset={} type={} wiki={} title='{}'",
                record.partition(), record.offset(), dto.type(), dto.wiki(), dto.title());
    }

    private WikimediaEventDto parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), WikimediaEventDto.class);
        } catch (JacksonException e) {
            // Malformed JSON will never parse, no matter how many times we retry.
            // IllegalArgumentException is registered as non-retryable in Lesson 20,
            // so this record skips backoff and goes straight to the dead-letter topic.
            throw new IllegalArgumentException(
                    "Unparseable Wikimedia event [partition=%d offset=%d]: %s"
                            .formatted(record.partition(), record.offset(), e.getMessage()), e);
        }
    }
}
```

Read that catch block carefully. It is doing three things:

**It converts an unchecked exception into a *meaningful* unchecked exception.** `JacksonException` says "the JSON was bad." `IllegalArgumentException` says "this record's argument is invalid and will always be invalid."

**It records where the bad record lives** — partition and offset — in the message. When you find this in a log at 3 a.m., that's how you fetch the payload.

**It chains the cause** with `, e`. Never swallow the original.

This exception type is a promise you'll keep in Lesson 20. Nothing enforces it yet; a malformed record right now would be retried forever by the default error handler.

### 4. Run it

```bash
./mvnw spring-boot:run
```

```
Consumed | partition=0 offset=483 type=edit wiki=commonswiki title='File:Prigioni cella.JPG'
Consumed | partition=2 offset=257 type=edit wiki=wikidatawiki title='Q15440113'
Consumed | partition=1 offset=690 type=categorize wiki=enwiki title='Category:Living people'
```

Structured fields instead of a string length. And the `type` column shows the four event kinds — `edit`, `new`, `log`, `categorize`.

### 5. Feed it garbage

Produce a malformed record by hand:

```bash
echo 'this is not json' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

Watch the consumer. It throws `IllegalArgumentException`, and then — because there's no error handler configured yet — Spring's default retries it, forever, roughly ten times a second.

**The partition is now blocked.** The consumer cannot advance past this record, so every valid record behind it on that partition waits. One bad byte has stopped a third of your pipeline.

Stop the consumer before your disk fills with logs. Skip the poison record by moving the group forward:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group --topic wikimedia-stream \
  --reset-offsets --to-latest --execute
```

That's Lesson 05's offset reset, used in anger. It works, and it's a terrible long-term answer — you just skipped *every* pending record on every partition to get past one.

Fixing this properly is Part 4.

---

## Try it yourself

1. Remove `ignoreUnknown = true` and restart. Which exception, and on which field? Now consider: Wikimedia adds a field next Tuesday. What breaks, and when do you find out?

2. Change `boolean bot` to `Boolean bot` and produce a record with `"bot": null`. Then change it back and produce the same record. Which version throws, and what does that tell you about primitives in DTOs?

3. Switch to Option A — set `value-deserializer` to Spring's `JsonDeserializer` with `spring.json.value.default.type`. Produce a malformed record. Where does the exception surface now, and can your listener see the raw bytes?

---

## Common mistakes

**Importing `com.fasterxml.jackson.databind.ObjectMapper` under Spring Boot 4.**
That's Jackson 2. Boot 4 ships Jackson 3: `tools.jackson.databind.ObjectMapper`. The annotations, confusingly, stay on `com.fasterxml`.

**Catching `JsonProcessingException`.**
It doesn't exist in Jackson 3. Catch `JacksonException`, which is unchecked — so nothing forces you to catch it at all.

**Omitting `ignoreUnknown = true` on an externally-owned payload.**
Every new field the producer adds becomes an outage for you.

**Constructing a `new ObjectMapper()` in your service.**
Inject the auto-configured bean. Yours won't have Boot's modules, date handling, or configuration, and you'll create one per instance.

**Letting a `JacksonException` propagate unwrapped.**
Its type tells the error handler nothing about whether retrying is useful. Wrap it in something that means "never retry this."

**Using Lombok `@Data` on a DTO.**
Gives you a mutable event with setters. Use a record.

---

## Check your understanding

**1. Why does `@JsonIgnoreProperties(ignoreUnknown = true)` matter more for a Kafka consumer than for a REST controller handling your own API?**

<details>
<summary>Reveal answer</summary>

Because of who controls the schema, and when you find out it changed.

In your own REST API, request and response types are versioned together and deployed together. In Kafka, the producer is a separate service — here, a separate *organisation* — that can add a field at any moment. The topic may also hold seven days of records written by three different producer versions.

Without `ignoreUnknown`, the first record containing a new field throws `UnrecognizedPropertyException` on the consumer, which retries it, which blocks the partition. A field addition — the most backwards-compatible change there is — becomes an outage.

Consumers must tolerate fields they don't know about. That's the same principle Schema Registry enforces formally in Lesson 25.

</details>

**2. Why wrap `JacksonException` in `IllegalArgumentException` rather than just letting it propagate?**

<details>
<summary>Reveal answer</summary>

Because the exception *type* is the signal your error handler will use to decide whether retrying makes sense.

In Lesson 20 you'll register `IllegalArgumentException` as non-retryable. A record that can't be parsed will never parse — the bytes don't change between attempts — so retrying is pure waste that blocks the partition for the duration of the backoff.

`JacksonException` alone doesn't carry that meaning: Spring's error handler has no opinion about it and would apply the default retry policy. By translating "the JSON is bad" into "this argument is permanently invalid," you make a routing decision explicit in the type system.

The wrapping also lets you attach the partition and offset, and preserve the original cause.

</details>

**3. Spring Boot 4 changed `ObjectMapper` to `tools.jackson.databind` but left `@JsonProperty` on `com.fasterxml.jackson.annotation`. Why the split?**

<details>
<summary>Reveal answer</summary>

Because the *annotations* are a stable, widely-depended-upon API, while the databind implementation had breaking changes to make.

Jackson 3 changed package names to allow it to coexist with Jackson 2 on the same classpath — essential during migration, when some libraries you depend on still use Jackson 2. If the annotations had also moved, every annotated DTO in the ecosystem would need editing, and a class annotated for Jackson 2 could not be read by Jackson 3.

Keeping `jackson-annotations` on its old coordinates means DTOs are source-compatible across both majors. Only code touching `ObjectMapper` and the exception hierarchy needs to change — which, in this project, is exactly one method.

</details>

**4. You produce `this is not json` to the topic. Your consumer has no error handler. Describe precisely what happens to the other records on that partition.**

<details>
<summary>Reveal answer</summary>

They stop being processed, indefinitely.

The listener throws, so the offset is never committed. Spring's default error handler retries the same record — by seeking back to it and re-polling — many times per second. Since the record cannot parse, every attempt fails identically.

The consumer never advances past that offset, so **every valid record behind it on the same partition is never delivered.** With 3 partitions, one poison message stops a third of your throughput and lag on that partition grows without bound.

The other two partitions are unaffected — they're independent logs with independent offsets, likely handled by different consumer threads.

This is called a *poison pill*, and it's the single best argument for a dead-letter topic.

</details>

**5. Configuring Kafka's `JsonDeserializer` produces cleaner listener code. Name the specific capability you give up.**

<details>
<summary>Reveal answer</summary>

Access to the raw bytes at the moment of failure, and control over the exception type.

With `JsonDeserializer`, deserialization happens inside the Kafka consumer client, *before* the listener container invokes your method. A malformed record throws `SerializationException` from within `poll()` itself. Your listener is never called, so you cannot catch it there, cannot log the offending payload, and cannot choose an exception type that means "don't retry."

Recovering requires wrapping it in `ErrorHandlingDeserializer`, which catches the failure and passes a special marker to the error handler — workable, but an extra layer, and the original payload still has to be dug out of a header.

Taking the `String` and parsing it yourself keeps failure inside ordinary application code, where exceptions are yours to name.

</details>

---

## Recap

An immutable `record`, nine fields out of forty, `ignoreUnknown = true` so the producer can evolve freely. Parsing happens inside the listener — not in a configured deserializer — so that a malformed record produces an exception *you* chose, carrying the partition and offset, ready for an error handler that knows retrying it is pointless.

You also proved what happens without that error handler: one bad record blocks a partition forever.

Before fixing that, there's a more fundamental problem. Your consumer is committing offsets automatically, on a timer, whether or not it actually did anything with the record.

**Next:** [Lesson 17 — Manual acknowledgment →](17-manual-acknowledgment.md)
