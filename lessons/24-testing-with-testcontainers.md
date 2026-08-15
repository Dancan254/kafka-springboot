# Lesson 24 — Testing Kafka with Testcontainers

> **Part 5 — Production** · 35 minutes

---

## What you'll learn

- Why mocking `KafkaTemplate` produces tests that pass while the pipeline is broken
- How `@ServiceConnection` wires a real broker into your Spring context with no properties
- How to test an asynchronous listener without `Thread.sleep`
- How to test the dead-letter path — the code most likely to be wrong and least likely to be tested

---

## Why this matters

Open `consumer/src/test/java/com/javaguy/consumer/ConsumerApplicationTests.java`. It contains one empty method named `contextLoads()`. So does the producer's. That is the entirety of this project's test coverage, and it is the entirety of most Kafka projects' test coverage.

It is not nothing — a context that fails to start is a real bug, and `contextLoads()` would have caught the missing `KafkaTemplate` bean from Lesson 08. But it verifies nothing about whether a record produced to a topic is consumed, parsed, persisted, and acknowledged.

The alternative most people reach for is mocking. A test that mocks `KafkaTemplate` and asserts `verify(template).send(...)` will pass with a broken serializer, an unreachable broker, a topic that doesn't exist, and a consumer that never commits. It tests that you called a method.

The house rule: **if you mock the infrastructure, the test is lying.** Kafka gets a real broker.

---

## Before you start

[Lesson 23](23-rest-api-over-events.md). Docker running — Testcontainers needs it.

---

## The concept

### Testcontainers, and why it isn't slow anymore

Testcontainers starts a real Kafka broker in a Docker container for your test, then throws it away.

The objection is startup time. Two things address it:

**A `static` container is started once per class**, not once per test method. JUnit 5 initialises static fields before any test runs, and Testcontainers' `@Container` on a static field ties its lifecycle to the class.

**Reuse across classes** is possible via singleton containers or `testcontainers.reuse.enable=true`. For a course, per-class is fine.

A modern KRaft Kafka container starts in a few seconds. That is a reasonable price for a test that actually proves your consumer works.

### `@ServiceConnection` — the part that changed everything

Before Spring Boot 3.1, wiring a container into the context meant this:

```java
@DynamicPropertySource
static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
}
```

Now:

```java
@Container
@ServiceConnection
static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.0");
```

`@ServiceConnection` tells Spring Boot to derive the connection details from the container automatically. No property names to misspell, and it works for Postgres, Redis, and the rest identically.

Note the two annotations do different jobs and you need both:

- **`@Container`** (with `@Testcontainers` on the class) starts and stops the container.
- **`@ServiceConnection`** wires its connection details into the Spring context.

Neither implies the other.

### Which `KafkaContainer`?

There are two classes with that name, and it matters.

| Class | Image | Status |
|---|---|---|
| `org.testcontainers.containers.KafkaContainer` | `confluentinc/cp-kafka` | legacy, ZooKeeper-era |
| **`org.testcontainers.kafka.KafkaContainer`** | `apache/kafka` | current, KRaft |

Use the second. The first still works and still drags in a ZooKeeper-shaped setup you spent Lesson 07 learning to avoid.

### Testing asynchronous code without sleeping

A `@KafkaListener` runs on a background thread. Produce a record and assert immediately, and you assert against an empty database.

```java
Thread.sleep(3000);   // don't
```

`Thread.sleep` is wrong in both directions: it makes a fast machine slow, and a slow machine flaky. The test suite gets slower every time someone bumps the number to fix a CI failure.

**Awaitility** polls until a condition holds, or fails after a timeout:

```java
await().atMost(Duration.ofSeconds(10))
       .untilAsserted(() -> assertThat(repository.count()).isEqualTo(1));
```

It returns as soon as the condition is true — typically in milliseconds — and fails with the actual assertion error, not a bare timeout.

### What gets an integration test

From the house rules: anything touching Kafka, the database, or an external system.

Concretely, three things are worth testing here, in increasing order of value:

1. **The happy path** — produce a valid record, assert a row appears.
2. **The offset commit** — assert the consumer group's committed offset advanced. Without this, a listener that never acknowledges still passes test 1.
3. **The dead-letter path** — produce a malformed record, assert it lands on `wikimedia-stream.dlt`. This is the code that only runs during incidents. It is the least exercised and the most important.

---

## Hands-on

### 1. Test dependencies

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
</dependency>

<!-- @ServiceConnection support -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-testcontainers</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.awaitility</groupId>
    <artifactId>awaitility</artifactId>
    <scope>test</scope>
</dependency>
```

Versions come from Spring Boot's BOM — Testcontainers and Awaitility are both managed. Don't pin them yourself.

### 2. The happy path

`src/test/java/com/javaguy/consumer/WikimediaConsumerIntegrationTest.java`:

```java
package com.javaguy.consumer;

import com.javaguy.consumer.repository.WikimediaEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class WikimediaConsumerIntegrationTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.0");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private WikimediaEventRepository repository;

    @BeforeEach
    void clearDatabase() {
        repository.deleteAll();
    }

    @Test
    void should_persist_event_when_valid_record_is_consumed() {
        String payload = """
                {"type":"edit","title":"Nikola Tesla","user":"alice","bot":false,
                 "namespace":0,"wiki":"enwiki","server_name":"en.wikipedia.org",
                 "timestamp":1704067200,"comment":"fixed a typo"}
                """;

        kafkaTemplate.send("wikimedia-stream", payload);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.count()).isEqualTo(1);
            var event = repository.findAll().getFirst();
            assertThat(event.getTitle()).isEqualTo("Nikola Tesla");
            assertThat(event.getWiki()).isEqualTo("enwiki");
            assertThat(event.isBot()).isFalse();
        });
    }
}
```

Points worth noticing.

**No `@MockBean` anywhere.** A real broker, a real listener container, a real H2 database, and a real Jackson `ObjectMapper`. The only thing faked is the source of the record, which is the thing under test.

**`static` container.** One broker per test class, started before the Spring context so `@ServiceConnection` has an address to hand over.

**`@BeforeEach` truncates the database**, not the topic. Topics are append-only — you can't clean them. Which is a real constraint on Kafka testing, and the reason each test class gets a fresh broker.

**`should_<expected>_when_<condition>`**, readable as documentation.

### 3. Test the acknowledgment

The happy-path test passes even if `acknowledge()` is never called — the row still gets written. To catch that, assert the offset moved.

```java
    @Test
    void should_commit_offset_after_persisting() {
        kafkaTemplate.send("wikimedia-stream", validPayload());

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(repository.count()).isEqualTo(1));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            try (var admin = AdminClient.create(Map.of(
                    BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {

                var offsets = admin.listConsumerGroupOffsets("wikimedia-consumer-group")
                        .partitionsToOffsetAndMetadata().get();

                assertThat(offsets.values())
                        .as("consumer group should have committed at least one offset")
                        .anySatisfy(meta -> assertThat(meta.offset()).isPositive());
            }
        });
    }
```

Delete the `acknowledgment.acknowledge()` line from `WikimediaConsumer` and this test fails while the first one still passes. That gap is exactly the bug Lesson 17 was about.

### 4. Test the dead-letter path

The valuable one.

```java
    @Test
    void should_route_malformed_record_to_dead_letter_topic() {
        kafkaTemplate.send("wikimedia-stream", "this is not json");

        try (var consumer = dltConsumer()) {
            consumer.subscribe(List.of("wikimedia-stream.dlt"));

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                var records = consumer.poll(Duration.ofMillis(500));
                assertThat(records.count()).isEqualTo(1);

                var record = records.iterator().next();
                assertThat(record.value()).isEqualTo("this is not json");

                var exceptionClass = new String(
                        record.headers().lastHeader("kafka_dlt-exception-fqcn").value(),
                        StandardCharsets.UTF_8);
                assertThat(exceptionClass).isEqualTo("java.lang.IllegalArgumentException");
            });
        }

        assertThat(repository.count())
                .as("malformed record must not be persisted")
                .isZero();
    }

    private KafkaConsumer<String, String> dltConsumer() {
        return new KafkaConsumer<>(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                GROUP_ID_CONFIG, "test-dlt-reader-" + UUID.randomUUID(),
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
```

This asserts four separate promises made across Lessons 16, 20, 21, and 22:

- the malformed record reached the DLT (Lesson 21's recoverer is wired)
- its value is the **original bytes**, unmodified
- the exception was `IllegalArgumentException`, so Lesson 16's wrapping happened and Lesson 20's non-retryable classification fired
- nothing was persisted

A random `GROUP_ID` per run means the test consumer never inherits a committed offset from a previous execution. Reusing a fixed group id across runs is a classic source of a test that passes once and then fails forever.

### 5. Why the timeout is 15 seconds here

The happy path resolves in milliseconds. The DLT path does not, and the number encodes something real.

`IllegalArgumentException` is registered non-retryable, so there's no backoff — that path is fast. But if you removed that registration, the record would take the full `ExponentialBackOff` schedule from Lesson 20: four attempts over seven seconds before the recoverer runs.

A 15-second timeout accommodates both. A 2-second timeout would pass today and fail the moment someone changes the exception classification — which is a test that fails for the *right reason* but with a confusing message.

### 6. Run them

```bash
./mvnw test
```

The first run pulls `apache/kafka:3.9.0`. Subsequent runs start the container in a few seconds.

```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
```

Now break something on purpose. Comment out `factory.setCommonErrorHandler(errorHandler)` in `KafkaConsumerConfig` and re-run. The DLT test fails: no record ever reaches the dead-letter topic, because the recoverer was never wired.

That is a test earning its keep. `contextLoads()` would have passed.

---

## Try it yourself

1. Delete `acknowledgment.acknowledge()`. Which of the three tests fail? Is the happy-path test still green? What does that tell you about what a "passing test" proves?

2. Add a test for `GET /api/v1/wikimedia/events/stats` using `@SpringBootTest(webEnvironment = RANDOM_PORT)` and `TestRestTemplate`. Seed the database with `@Sql` rather than by producing to Kafka. Why is that the right choice for a controller test?

3. Add a `PostgreSQLContainer` with `@ServiceConnection` alongside the Kafka one, and drop H2. How many properties did you have to configure? Now do it the old way with `@DynamicPropertySource` and count again.

4. Make the DLT test assert on `kafka_dlt-original-offset`. Remember from Lesson 22 that it's an 8-byte big-endian long — write the assertion so it fails loudly if someone decodes it as a `String`.

---

## Common mistakes

**Mocking `KafkaTemplate` or the repository.**
The test then passes with a broken serializer, a missing topic, an unreachable broker, or a listener that never commits. It tests that you called a method.

**Using `org.testcontainers.containers.KafkaContainer`.**
The legacy, ZooKeeper-era class. Use `org.testcontainers.kafka.KafkaContainer` with the `apache/kafka` image.

**`@Container` without `@ServiceConnection`, or vice versa.**
The first starts the container; the second wires it into Spring. You need both.

**A non-static `@Container`.**
Starts and stops a broker per test method. Your suite takes minutes.

**`Thread.sleep` to wait for a listener.**
Slow when it passes, flaky when it doesn't. Use Awaitility.

**A fixed `group.id` in a test consumer.**
It commits offsets that persist for the container's lifetime. The test passes on a fresh broker and then reads nothing.

**Trying to clean a topic between tests.**
You can't. Topics are append-only. Reset the database, use a fresh group id, or use a fresh container.

---

## Check your understanding

**1. Your test mocks `KafkaTemplate` and asserts `verify(kafkaTemplate).send("wikimedia-stream", payload)`. Name three production bugs it cannot catch.**

<details>
<summary>Reveal answer</summary>

Any number, but the obvious ones:

- **The topic doesn't exist** and auto-creation is off. The mock doesn't care.
- **The serializer is wrong** — you configured `JsonDeserializer` on the consumer while producing with `StringSerializer`. The mock never serializes anything.
- **The listener never acknowledges.** The mock has no consumer at all.
- **`acks=all` cannot be satisfied** because `min.insync.replicas` exceeds the replica count. Never exercised.
- **The DLT is misconfigured** with fewer partitions than the source, so the recoverer throws.

The mock verifies that a method was called with certain arguments. Every one of those bugs lives *after* that method call, in the parts you replaced with a mock.

This is what "if you mock the infrastructure, the test is lying" means — it's not that mocks are bad, it's that Kafka *is* the thing under test here.

</details>

**2. Why must the `@Container` field be `static`?**

<details>
<summary>Reveal answer</summary>

So the container starts once per test **class** rather than once per test **method**, and — critically — before the Spring context is created.

JUnit 5 initialises static fields during class loading, before any instance or extension callback runs. Testcontainers' JUnit extension starts a static `@Container` in `beforeAll`, non-static ones in `beforeEach`.

Spring's context is also built once per class (and cached). `@ServiceConnection` needs to read `kafka.getBootstrapServers()` when the context is being built — which means the container must already be running. A non-static container hasn't started yet at that point.

So a non-static `@Container` both breaks `@ServiceConnection` and starts a fresh broker per test method, turning a 5-second suite into a 5-minute one.

</details>

**3. The happy-path test passes. You then delete `acknowledgment.acknowledge()`. Does it still pass, and what does that reveal?**

<details>
<summary>Reveal answer</summary>

It still passes.

The listener still parses the record and still calls `repository.save()`. The row appears, the count is 1, the assertion holds. All the acknowledgment does is commit the offset — a fact entirely invisible to the database.

What actually happens in production: the record is never committed, so on the next poll it's redelivered and processed again, forever. An infinite loop of duplicate rows, and a consumer group whose lag never decreases.

The happy-path test cannot see it because it checks the *effect* of processing, not the *completion* of it. That's why the second test exists, asserting against `AdminClient.listConsumerGroupOffsets`. Testing that a side effect occurred is not the same as testing that the unit of work finished.

</details>

**4. Why does the DLT test use a randomly-generated `group.id`?**

<details>
<summary>Reveal answer</summary>

Because a consumer group's committed offsets outlive the consumer, and Testcontainers reuses the broker across all methods in the class.

With a fixed group id like `"test-dlt-reader"`, the first test method reads the DLT record and commits offset 1. A later method — or a re-run against a reused container — creates a consumer in the same group, finds a committed offset of 1, and starts from there. `auto.offset.reset: earliest` is ignored, because the group *has* an offset (Lesson 15).

The consumer polls, gets nothing, and the test fails with a timeout that looks like "the DLT is broken."

A fresh `UUID` per run guarantees a brand-new group with no committed offset, so `earliest` applies and the consumer sees everything on the topic.

</details>

**5. You cannot truncate a Kafka topic between tests the way you truncate a database table. Why, and what do you do instead?**

<details>
<summary>Reveal answer</summary>

Because a topic is an append-only log. There is no `DELETE`. Retention deletes old *segments* on a schedule, and `kafka-delete-records` moves the log-start offset forward — neither is a synchronous, per-test cleanup primitive.

The practical approaches, in order:

- **Fresh container per test class.** Simple, and what this lesson does. State cannot leak between classes.
- **Fresh consumer group per test method** (random `group.id`), so each reader starts from `earliest` regardless of what previous tests committed.
- **Unique topic names per test**, if you need isolation within a class.
- **Reset the database in `@BeforeEach`**, since *that* you can truncate.

The deeper point: Kafka's core property — that reads don't destroy and the log is immutable — is exactly what makes it excellent in production and awkward in tests. You design around it rather than fight it.

</details>

---

## Recap

A real broker in a container, wired in by `@Container` plus `@ServiceConnection`, with `static` so it starts once and before the Spring context. Awaitility instead of `Thread.sleep`. No mocks of the infrastructure under test.

Three tests, each proving a different promise: the record is persisted, the offset is committed, and a malformed record reaches the dead-letter topic with its original bytes and the right exception class. The last one exercises the code path that only ever runs during an incident.

Everything so far has moved plain strings. Now let's put a contract on the wire.

**Next:** [Lesson 25 — Schema Registry & Avro →](25-schema-registry-and-avro.md)
