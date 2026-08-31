# Lesson 16: DTO Records and Deserialization

> **Part 3: The Consumer**

---

## What you'll learn

- Why you should bind only the fields you need from a payload you do not own
- Why an inbound event is a `record` rather than a class
- What Jackson 3 changed in Spring Boot 4, and what it did not
- Why parsing inside the listener beats configuring a JSON deserializer

---

## Why this matters

Your consumer logs the length of a string. That is not processing, it is proof of delivery.

Turning the payload into a typed object is where the interesting decisions live, and most of them are about coupling. You are consuming a schema owned by someone else, who can change it without telling you, and the choices in this lesson decide whether that breaks you.

---

## Before you start

[Lesson 15](15-first-kafkalistener.md), with a consumer that logs records.

Look at the payload before modelling it:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream --max-messages 1
```

A large JSON document containing `$schema`, `meta`, `id`, `type`, `namespace`, `title`, `comment`, `timestamp`, `user`, `bot`, `server_name`, `wiki` and more.

---

## The concept

### Take only what you need

You could map all forty fields. Do not.

Every field you bind is a field you have coupled yourself to. Wikimedia can add, rename or remove fields at any time, and you do not control their schema. Bind nine, ignore the rest, and their changes cannot break you unless they touch one of your nine.

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikimediaEventDto(...) {}
```

Without `ignoreUnknown = true`, Jackson fails on the first unmapped field. With it, unmapped fields are discarded. For a payload you do not own, this is not laziness. It is the only sane default.

### Records, not classes

An inbound event is immutable data with no behaviour, lifecycle or identity, which is exactly what a `record` is for. You get a constructor, accessors, `equals`, `hashCode` and `toString`, and you cannot accidentally mutate an event halfway through processing.

Jackson binds to records natively, using the canonical constructor, so there is no no-argument constructor and no setters to expose.

> This is also why this course puts no Lombok on DTOs. `@Data` on a class gives you a mutable object with setters, which is the opposite of what an event should be. Records make Lombok unnecessary here. Lesson 18 is where Lombok earns its place.

### Field names that do not match

Wikimedia sends `server_name` and Java wants `serverName`:

```java
@JsonProperty("server_name") String serverName
```

You could configure a global snake-case naming strategy instead, but that applies to every type in the application and couples your whole codebase to one external system's convention. An annotation on the single field that differs is more honest.

### Jackson 3, a Spring Boot 4 surprise

Spring Boot 4 ships Jackson 3, which changed its package names:

| | Jackson 2 | Jackson 3 |
|---|---|---|
| `ObjectMapper` | `com.fasterxml.jackson.databind` | `tools.jackson.databind` |
| Core exception | `com.fasterxml.jackson.core.JsonProcessingException` | `tools.jackson.core.JacksonException` |
| Annotations | `com.fasterxml.jackson.annotation` | unchanged |

Two things follow.

**The annotations still import from `com.fasterxml.jackson.annotation`.** `@JsonProperty` and `@JsonIgnoreProperties` live in `jackson-annotations`, which kept its coordinates, so a DTO written for Jackson 2 needs no changes at all. This is confusing the first time: one file with imports from two different roots, both correct.

**`JsonProcessingException` is gone.** In Jackson 3, `JacksonException` extends `RuntimeException`, so parsing failures are unchecked. Code that used to `catch (JsonProcessingException e)` no longer compiles, and code that caught nothing now compiles and throws at runtime.

You also need the dependency explicitly, because Boot 4 splits Jackson into its own starter rather than pulling it in through the Kafka starter. You added it to the producer in Lesson 14 for the same reason.

### Where to deserialize

There are two places, and the choice matters more than it looks.

**Option A, configure Kafka's `JsonDeserializer`:**

```yaml
value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
```

The record arrives as a `WikimediaEventDto` directly. Clean, and wrong for this pipeline.

Deserialization now happens before your listener is invoked, inside the Kafka client. A malformed record throws a `SerializationException` your listener never sees, and which the error handler in Lesson 20 cannot route usefully. Handling it requires wrapping in an `ErrorHandlingDeserializer`, which adds indirection exactly where you want clarity.

Worse, the raw bytes are gone by the time you learn there was a problem. When you later want to inspect the bad payload, you have a stack trace and no data.

**Option B, keep `StringDeserializer` and parse in the listener.** The raw JSON is a `String` you still hold, so a parse failure is an ordinary exception thrown from your own code, with the partition, offset and payload all in scope. That is what makes the dead-letter topic in Lesson 21 useful rather than merely tidy.

This course uses option B throughout. Option A is a reasonable choice for internal topics whose schema you own and where a malformed record indicates a bug rather than an expected event.

---

## Hands-on

### 1. Add Jackson

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jackson</artifactId>
        </dependency>
```

This provides the auto-configured `ObjectMapper` bean you are about to inject.

### 2. `dto/WikimediaEventDto.java`

```java
package com.example.wikimedia.consumer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Maps only the fields this consumer uses. Unrecognised fields are discarded so
 * that Wikimedia can evolve its payload without breaking us.
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

Nine fields out of roughly forty. `namespace` and `timestamp` are boxed because Wikimedia may omit them, and `bot` is a primitive because it is always present. That distinction is worth making deliberately: a primitive silently becomes `false` when the field is absent, which is a lie if absence meant "unknown".

### 3. Parse inside the listener

```java
package com.example.wikimedia.consumer.kafka;

import com.example.wikimedia.consumer.dto.WikimediaEventDto;
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
        WikimediaEventDto event = parse(record);

        log.info("Consumed partition={} offset={} type={} wiki={} title='{}'",
                record.partition(), record.offset(), event.type(), event.wiki(), event.title());
    }

    private WikimediaEventDto parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), WikimediaEventDto.class);
        } catch (JacksonException e) {
            // Malformed JSON will never parse, however many times it is retried.
            // IllegalArgumentException is registered as non-retryable in Lesson 20,
            // so this record skips backoff and goes straight to the dead-letter topic.
            throw new IllegalArgumentException(
                    "Unparseable Wikimedia event [partition=%d offset=%d]: %s"
                            .formatted(record.partition(), record.offset(), e.getMessage()), e);
        }
    }
}
```

That catch block is doing three things worth naming.

**It converts an unchecked exception into a meaningful one.** `JacksonException` says the JSON was bad. `IllegalArgumentException` says this record's content is invalid and always will be, which is a statement about retry policy rather than about parsing.

**It records where the bad record lives.** Partition and offset in the message are how you fetch the payload later.

**It chains the cause.** Never discard the original.

This exception type is a promise you keep in Lesson 20. Nothing enforces it yet, so a malformed record right now would be retried indefinitely by the default error handler, which step 5 demonstrates.

### 4. Run it

```bash
./mvnw spring-boot:run
```

```
Consumed partition=0 offset=483 type=edit wiki=commonswiki title='File:Prigioni cella.JPG'
Consumed partition=2 offset=257 type=edit wiki=wikidatawiki title='Q15440113'
Consumed partition=1 offset=690 type=categorize wiki=enwiki title='Category:Living people'
```

Structured fields instead of a string length, and the `type` column shows the four event kinds the feed emits: `edit`, `new`, `log` and `categorize`.

### 5. Feed it garbage

Produce a malformed record by hand:

```bash
echo 'this is not json' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

Watch the consumer log. You will see the same `IllegalArgumentException` reported over and over, because the default error handler retries and the record can never succeed.

Worse, the consumer is now stuck on that offset. Records behind it on the same partition are not being processed, and the group's lag for that partition grows without limit.

This is a **poison pill**, and it is the single most common way a Kafka consumer stops working in production. Nothing crashed, the process is healthy, the other partitions are fine, and one partition is frozen forever.

Lessons 20, 21 and 22 exist to fix exactly this. For now, get unstuck by skipping the record:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group \
  --topic wikimedia-stream \
  --reset-offsets --shift-by 1 --execute
```

Stop the consumer before running that, since Lesson 05 showed a group must have no active members to be reset. Then start it again.

Note what you just did in production terms: you skipped a record without knowing what was in it. That is the manual version of a dead-letter topic, and it is why the automated version is worth building.

---

## Try it yourself

1. Add a field to the DTO that Wikimedia does not send, such as `String nonexistent`. Run it. Does anything fail? Now remove `@JsonIgnoreProperties` and run again. Which of the two directions of schema mismatch does that annotation protect you from?

2. Change `boolean bot` to `Boolean bot` and produce a record with the `bot` field missing. Compare the parsed value in each case, and decide which is correct for a field that means "was this edit automated".

3. Switch to option A by setting `value-deserializer` to Spring's `JsonDeserializer` with the appropriate trusted-packages property, then repeat step 5's garbage record. Where does the exception surface now, and what can you still learn about the bad payload?

4. Wikimedia nests useful data under `meta`, including `domain` and `uri`. Add a nested record for it and bind two of those fields. Then explain what your consumer is now coupled to that it was not before.

---

## Common mistakes

**Binding every field in a payload you do not own.**
Every bound field is a coupling. Bind what you use.

**Omitting `ignoreUnknown = true` on an external payload.**
The first field the publisher adds breaks your consumer, and it will be their routine Tuesday.

**Using Lombok `@Data` for an event.**
You get mutability and setters on something that should be immutable. Use a record.

**Importing `tools.jackson.annotation`.**
The annotations did not move. Only `ObjectMapper` and the exceptions did.

**Catching `JsonProcessingException`.**
It no longer exists in Jackson 3. `JacksonException` is unchecked, so nothing forces you to handle it and nothing warns you that you have not.

**Letting a parse failure escape without context.**
Without the partition and offset in the message, you cannot find the record that caused it.

**Assuming a poison pill is loud.**
The process stays healthy and only one partition stops. Lag on that partition is the only signal.

---

## Check your understanding

**1. Wikimedia adds a new field to its payload tomorrow. Does your consumer break?**

<details>
<summary>Reveal answer</summary>

No, because of `@JsonIgnoreProperties(ignoreUnknown = true)`. Unmapped fields are discarded.

Without it, Jackson would treat an unrecognised property as an error and every record would fail to parse, which on a feed you do not control is a self-inflicted outage triggered by someone else's routine change.

The direction that would still break you is a field you *do* bind being removed or changed in type. That is the coupling you accepted deliberately, and it is why the DTO binds nine fields rather than forty.

</details>

**2. Why parse inside the listener rather than configure a `JsonDeserializer`?**

<details>
<summary>Reveal answer</summary>

Because it keeps the failure inside your own code, where the raw payload still exists.

With a configured deserializer, a malformed record fails inside the Kafka client before your method is called. You get a `SerializationException` with no access to the record, the error handler cannot treat it like an application exception, and routing it to a dead-letter topic requires an `ErrorHandlingDeserializer` wrapper.

Parsing in the listener means a bad record is an ordinary exception thrown from your code, with the partition, offset and full JSON in scope. That is what makes Lesson 21's dead-letter topic contain something worth reading.

</details>

**3. Step 5 produced one bad record and the consumer stopped making progress on that partition. Why did the other partitions keep working?**

<details>
<summary>Reveal answer</summary>

Because offsets and ordering are per partition, and so is being stuck.

The container cannot advance past a record that keeps failing, since committing a later offset would mean silently accepting that the record was skipped. So that partition's position stays where it is while the record is retried.

The other partitions have their own positions and are unaffected, which is exactly what makes this failure hard to notice. Total throughput drops by roughly a third, the process is healthy, no listener has crashed, and only per-partition lag reveals it.

</details>

**4. `boolean bot` is a primitive. What happens if the field is absent from a record?**

<details>
<summary>Reveal answer</summary>

It becomes `false`, silently, because that is the default value of a primitive `boolean`.

If absence genuinely means "not a bot" that is fine. If absence means "we do not know", you have just converted missing data into a confident negative, and every downstream count of human versus automated edits is quietly wrong.

`Boolean` would give you `null` instead, forcing the ambiguity to be handled rather than assumed. Choosing between them is a modelling decision about what absence means, not a style preference.

</details>

**5. Your parse failure throws `IllegalArgumentException` rather than letting `JacksonException` propagate. What does that buy you, given that neither is checked?**

<details>
<summary>Reveal answer</summary>

It states a retry policy in the type system, which is what Lesson 20 will read.

`JacksonException` describes a cause: the bytes were not valid JSON. `IllegalArgumentException` describes a consequence: this input is invalid and will be invalid on every attempt. The error handler in Lesson 20 classifies exceptions into retryable and not, and this is how you tell it which bucket this failure belongs in.

It also gives you a message you control, carrying the partition and offset, and it keeps the original as the cause so nothing is lost. Letting the raw Jackson exception escape would work, but it would leave the retry decision to whatever the framework guesses.

</details>

---

## Recap

You bound nine fields of a forty-field payload into an immutable record, ignoring the rest so the publisher can evolve their schema without breaking you. Jackson 3 moved `ObjectMapper` to `tools.jackson` and made its exceptions unchecked, while leaving the annotations where they were.

You parse inside the listener so that a bad record is your exception, with the payload still in hand, and you translate it into an exception type that states a retry policy.

Then you produced a poison pill and watched one partition stop forever, which is the problem Part 4 solves.

**Next:** [Lesson 17: Manual Acknowledgment](17-manual-acknowledgment.md)
