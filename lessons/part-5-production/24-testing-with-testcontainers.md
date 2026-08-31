# Lesson 24: Testing Kafka with Testcontainers

> **Part 5: Production**

---

## What you'll learn

- Why a real broker in a container beats an embedded one or a mock
- What `@ServiceConnection` does, and what it does not
- How to test an asynchronous pipeline without sleeping
- How to test the dead-letter path, including the header that catches everyone

---

## Why this matters

Your consumer has three behaviours that only exist at runtime: it acknowledges after persisting, it absorbs redelivery through a unique constraint, and it routes unparseable records to a dead-letter topic. None of those are visible in a unit test, because none of them are logic. They are configuration and framework behaviour.

A test that mocks `KafkaTemplate` verifies that you called a method. A test against a real broker verifies that the pipeline works.

---

## Before you start

[Lesson 23](23-rest-api-over-events.md), and Docker running. Your consumer project currently has one generated test that asserts the context loads.

---

## The concept

### Why a real broker

There have been three approaches to testing Kafka code, and only one of them is worth your time now.

**Mocking `KafkaTemplate`** tests that your code called `send`. It cannot tell you whether the record was serialisable, whether the topic existed, whether the key hashed where you expected, or whether the listener would have received it.

**`EmbeddedKafkaBroker`**, from `spring-kafka-test`, runs a broker in your JVM. It is fast and it is a different implementation from the one you deploy, with its own configuration surface and its own quirks.

**Testcontainers** starts the real broker image in Docker for the duration of the test. Same image, same version, same behaviour as your Compose stack. It costs a few seconds of startup and removes an entire class of "works in tests, fails in production".

### Testcontainers 2.x renamed everything

This is where most existing material will break your build. Testcontainers 2.0 renamed every module artifact with a `testcontainers-` prefix:

| Old | Current |
|---|---|
| `org.testcontainers:kafka` | `org.testcontainers:testcontainers-kafka` |
| `org.testcontainers:junit-jupiter` | `org.testcontainers:testcontainers-junit-jupiter` |
| `org.testcontainers:postgresql` | `org.testcontainers:testcontainers-postgresql` |

The old identifiers are absent from the BOM that Spring Boot imports, so a versionless declaration using them fails with a missing-version error rather than a helpful message.

The container classes moved too. `org.testcontainers.containers.KafkaContainer` was **removed**, and there are now two replacements in their own packages:

- `org.testcontainers.kafka.KafkaContainer` for `apache/kafka` images
- `org.testcontainers.kafka.ConfluentKafkaContainer` for `confluentinc/cp-kafka` images

Spring Boot 4 ships a connection-details factory for each, which is what makes `@ServiceConnection` work. Construct them with `DockerImageName.parse(...)` rather than a bare string, because the new classes are not self-generic and the string constructor is gone.

### `@ServiceConnection`

```java
@Container
@ServiceConnection
static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
```

Two annotations doing two different jobs, and they are frequently conflated.

`@Container`, from the `@Testcontainers` JUnit extension, manages the container lifecycle: start it before the tests, stop it after.

`@ServiceConnection`, from Spring Boot, reads the started container and contributes connection details to the application context. For a Kafka container that means `spring.kafka.bootstrap-servers` is set to the container's mapped address, with no property file and no `@DynamicPropertySource`.

The container field is `static` so one container is shared by every test in the class. A container per test method would work and would add several seconds each.

### The replication factor problem

Here is a trap specific to this pipeline, and it will stop your tests before any assertion runs.

Lesson 21 declared the dead-letter topic with `.replicas(3)` and a floor of 2. Lesson 09 did the same for the source topic. A single-node test container has one broker, so `KafkaAdmin` cannot create either topic and fails at startup with an invalid replication factor.

Worse, `acks=all` with a floor of 2 could never be satisfied on one broker even if the topics existed, which is exactly the failure Lesson 06 taught you to recognise.

The fix is a test profile that overrides both, and it is worth understanding as a general point: topic declarations that encode production durability need an environment-specific override, or your tests need a three-broker cluster.

### Testing asynchrony without sleeping

The pipeline is asynchronous. You produce a record and the listener runs on another thread at some later point.

`Thread.sleep(2000)` in a test is both slow and flaky: too short and it fails on a loaded machine, too long and every run pays for it.

Awaitility polls until a condition holds or a timeout expires:

```java
await().atMost(Duration.ofSeconds(20))
       .untilAsserted(() -> assertThat(repository.count()).isEqualTo(1));
```

It returns as soon as the condition is true, so the happy path is fast and the failure path is bounded. It is already on your classpath, because `spring-boot-starter-test` brings it in.

---

## Hands-on

### 1. Add the test dependencies

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
```

Three dependencies, no versions. Spring Boot's BOM manages Testcontainers, currently 2.0.5, so pinning them yourself is how you end up with a version that disagrees with the connection-details factories.

Awaitility is deliberately absent. It arrives transitively through `spring-boot-starter-test`, and declaring it again is harmless noise.

### 2. Override the replication factor for tests

`src/test/resources/application-test.yml`:

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest

  jpa:
    hibernate:
      ddl-auto: create-drop

# Read by the NewTopic beans so a single-broker test container can satisfy them.
wikimedia:
  topic:
    replicas: 1
    min-insync-replicas: 1
```

Then make the topic declarations use those values instead of hard-coded numbers. In the consumer's `WikimediaTopicConfig`:

```java
@Configuration
public class WikimediaTopicConfig {

    @Bean
    public NewTopic wikimediaStreamDltTopic(
            @Value("${wikimedia.topic.replicas:3}") int replicas,
            @Value("${wikimedia.topic.min-insync-replicas:2}") String minInSync) {

        return TopicBuilder
                .name("wikimedia-stream.dlt")
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
                .build();
    }
}
```

The defaults keep production behaviour, so nothing changes when the property is absent. Add `import org.springframework.beans.factory.annotation.Value;`.

This is the honest fix. The alternative, running three brokers in tests, is slower and mostly proves things you already proved by hand in Lesson 06.

### 3. The test class

`src/test/java/com/example/wikimedia/consumer/WikimediaConsumerIntegrationTest.java`:

```java
package com.example.wikimedia.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.wikimedia.consumer.repository.WikimediaEventRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class WikimediaConsumerIntegrationTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private WikimediaEventRepository repository;

    @BeforeEach
    void clearDatabase() {
        repository.deleteAll();
    }

    @Test
    void should_persist_event_when_valid_record_is_produced() {
        kafkaTemplate.send("wikimedia-stream", "Nikola Tesla", validPayload("Nikola Tesla"));

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(repository.count()).isEqualTo(1));

        var stored = repository.findAll().getFirst();
        assertThat(stored.getTitle()).isEqualTo("Nikola Tesla");
        assertThat(stored.getType()).isEqualTo("edit");
        assertThat(stored.getKafkaOffset()).isNotNegative();
    }

    @Test
    void should_store_once_when_same_record_is_delivered_twice() {
        // Two sends of the same payload are two records at two offsets, so this
        // is not the redelivery case: it proves the constraint is on the record
        // identity rather than on the payload.
        kafkaTemplate.send("wikimedia-stream", "Berlin", validPayload("Berlin"));
        kafkaTemplate.send("wikimedia-stream", "Berlin", validPayload("Berlin"));

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(repository.count()).isEqualTo(2));
    }

    @Test
    void should_route_to_dead_letter_topic_when_payload_is_unparseable() {
        kafkaTemplate.send("wikimedia-stream", "bad", "this is not json");

        try (var consumer = dltConsumer()) {
            consumer.subscribe(java.util.List.of("wikimedia-stream.dlt"));

            ConsumerRecord<String, String> dead = await()
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> pollOne(consumer), r -> r != null);

            assertThat(dead.value()).isEqualTo("this is not json");

            // The primary exception header holds Spring's wrapper, and the
            // application exception is in the cause header. Asserting the other
            // way round is the most common mistake against a dead-letter topic.
            assertThat(header(dead, "kafka_dlt-exception-fqcn"))
                    .isEqualTo("org.springframework.kafka.listener.ListenerExecutionFailedException");
            assertThat(header(dead, "kafka_dlt-exception-cause-fqcn"))
                    .isEqualTo("java.lang.IllegalArgumentException");
            assertThat(header(dead, "kafka_dlt-original-topic"))
                    .isEqualTo("wikimedia-stream");
        }

        assertThat(repository.count()).isZero();
    }

    private ConsumerRecord<String, String> pollOne(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        return records.isEmpty() ? null : records.iterator().next();
    }

    private KafkaConsumer<String, String> dltConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-reader",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        var h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private String validPayload(String title) {
        return """
                {"type":"edit","title":"%s","user":"tester","bot":false,
                 "namespace":0,"wiki":"enwiki","server_name":"en.wikipedia.org",
                 "timestamp":1735689600,"comment":"integration test"}
                """.formatted(title);
    }
}
```

Four details that matter and are usually wrong in examples.

**`ConsumerConfig` constants are qualified, not statically imported.** Static-importing `BOOTSTRAP_SERVERS_CONFIG` from both `ConsumerConfig` and `AdminClientConfig` in one file is an ambiguous reference that will not compile. Referring to them through the class avoids the problem entirely.

**The dead-letter reader is a plain `KafkaConsumer` in a try-with-resources block.** The application already has a listener on that topic in a different group, so this reader gets its own group and its own copy of the records.

**`validPayload` is defined.** Examples that call a helper they never show do not compile, and this one has to match `WikimediaEventDto` field for field.

**The redelivery test is named for what it actually proves.** Two `send` calls are two records at two offsets, so the unique constraint does not fire and two rows are correct. Proving true idempotency requires the same offset delivered twice, which is what the exercises cover.

### 4. Run them

```bash
./mvnw test
```

The first run pulls `apache/kafka:4.3.1` and takes a couple of minutes. Subsequent runs start the container in a few seconds.

Three tests, one broker, one Spring context.

### 5. Break things deliberately

Each of these should fail, and the failure is the point.

Remove `acknowledgment.acknowledge()` from `WikimediaConsumer` and re-run. The first test still passes, because the row is written before the acknowledgment. Now add an assertion on the committed offset and it fails, which tells you something useful: a missing acknowledgment is invisible to a test that only checks the database.

Comment out `factory.setCommonErrorHandler(errorHandler)` and re-run. The dead-letter test fails, because without the handler wired in nothing publishes to the dead-letter topic. That is the exact mistake Lesson 20 warned about, now caught by a test.

Swap the two exception header assertions so that `kafka_dlt-exception-fqcn` is expected to hold `IllegalArgumentException`. It fails, with the actual value in the message. That failure is worth seeing once, because in production the same mistake is a query that silently returns nothing.

---

## Try it yourself

1. Test the durability path properly. Use `AdminClient` to read the committed offset for `wikimedia-consumer-group` after the first test, and assert it advanced. Remember not to static-import the config constants from two classes.

2. Prove real idempotency rather than the two-record case. Produce one record, wait for the row, then reset the group's offset backwards with `AdminClient` and restart the listener container. Assert the row count is unchanged.

3. Add a PostgreSQL container with `testcontainers-postgresql` and `@ServiceConnection`, replacing H2 in tests. Which bug class does this catch that H2 cannot? Consider the reserved keyword from Lesson 18.

4. Assert on the API from Lesson 23 as well, using `RestTestClient`. Note that `TestRestTemplate` moved to `org.springframework.boot.resttestclient` in Boot 4 and is not on the classpath by default, so `RestTestClient` is the path of least resistance.

---

## Common mistakes

**Using the old Testcontainers artifact identifiers.**
`org.testcontainers:kafka` and `org.testcontainers:junit-jupiter` are not in Boot's BOM. The build fails with a missing version.

**Importing `org.testcontainers.containers.KafkaContainer`.**
Removed in Testcontainers 2.x. Use `org.testcontainers.kafka.KafkaContainer` for `apache/kafka` images, or `ConfluentKafkaContainer` for `confluentinc/cp-kafka`.

**Pinning the Testcontainers version.**
Boot's BOM manages it, and overriding it can leave you with a version whose containers the connection-details factories do not recognise.

**Forgetting `@Testcontainers`.**
`@ServiceConnection` alone contributes connection details for a container that nothing ever started.

**Leaving production replication factors in place for tests.**
A single-node container cannot create a topic with three replicas, and startup fails before any test runs.

**Using `Thread.sleep` to wait for a listener.**
Slow when it passes and flaky when it does not. Awaitility bounds both.

**A non-static container field.**
A new broker per test method, and several seconds each.

**Asserting your exception on `kafka_dlt-exception-fqcn`.**
It holds `ListenerExecutionFailedException`.

---

## Check your understanding

**1. Why not `EmbeddedKafkaBroker`, given that it is faster?**

<details>
<summary>Reveal answer</summary>

Because it is a different broker from the one you deploy.

It runs in your test JVM, which makes it quick, and it has its own configuration surface and its own behavioural differences. Anything that depends on real broker behaviour, and this pipeline depends on several such things, is being tested against an approximation.

Testcontainers runs the same image you run in Compose. The cost is a few seconds of container startup, paid once per class, and what you get back is the removal of an entire category of failure that only appears after deployment.

</details>

**2. What is the division of labour between `@Container` and `@ServiceConnection`?**

<details>
<summary>Reveal answer</summary>

`@Container` is lifecycle and `@ServiceConnection` is wiring.

`@Container`, activated by the `@Testcontainers` extension, starts the container before the tests and stops it afterwards. Without it the field is just an object and nothing runs.

`@ServiceConnection` is Spring Boot's half. It inspects the running container, works out which connection details it provides, and contributes them to the application context, so `spring.kafka.bootstrap-servers` points at the container's mapped port with no property file involved.

Each is useless without the other, which is why omitting one produces a confusing failure rather than an obvious one.

</details>

**3. Your topics declare three replicas. The test container has one broker. What fails, and when?**

<details>
<summary>Reveal answer</summary>

`KafkaAdmin` fails while the application context is starting, with an invalid replication factor error, before any test method runs.

This is Lesson 03's third exercise arriving in a test suite. A replication factor is a count of copies on distinct brokers, so three is impossible on one.

Even if the topics existed, `acks=all` against a floor of two could never be satisfied, so every produce would be refused. Overriding both values through a test profile is the fix, and keeping the production values as defaults means nothing changes outside tests.

</details>

**4. The redelivery test produces the same payload twice and expects two rows. Does that contradict Lesson 18?**

<details>
<summary>Reveal answer</summary>

No, and the distinction is the whole point of the idempotency key.

Lesson 18's unique constraint is on the partition and offset, which identify the *record*, not on the payload. Two `send` calls create two records at two different offsets, so both are new and both are stored. That is correct behaviour: two identical edits genuinely occurring twice are two events.

What the constraint prevents is the same record being processed twice, which happens on redelivery after a crash or a rebalance. Reproducing that in a test means resetting the group's offset backwards rather than producing again, which is the second exercise.

Naming the test for what it proves rather than for what it looks like avoids a false sense of coverage.

</details>

**5. Why does asserting on the dead-letter exception headers belong in a test at all, rather than being a documentation note?**

<details>
<summary>Reveal answer</summary>

Because the failure mode is silence, and silence is exactly what tests are for.

If you query a dead-letter topic for your own exception class on the primary header, you get an empty result. Nothing errors. The empty result reads as "no failures of this kind", which is the most misleading possible answer during an incident.

An assertion pins the actual behaviour, so if a future Spring version stops wrapping, or starts recording the cause differently, the test fails and tells you. Documentation cannot do that, and a comment in a dashboard query certainly cannot.

It also documents the surprise for the next person, in a form that cannot drift away from the truth.

</details>

---

## Recap

Testcontainers runs the same broker image you deploy, so the tests exercise real behaviour rather than an approximation. `@Container` handles the lifecycle and `@ServiceConnection` contributes the connection details, and both are needed.

Testcontainers 2.x renamed every module artifact with a `testcontainers-` prefix and removed the old container class, which is where most existing examples break. Boot's BOM manages the version, so declare no version at all.

Production replication factors have to be overridden for a single-node container, Awaitility replaces sleeping, and the dead-letter test pins the exception header that would otherwise fail silently.

**Next:** [Lesson 25: Schema Registry and Avro](25-schema-registry-and-avro.md)
