# Lesson 18: Persisting with JPA

> **Part 3: The Consumer**

---

## What you'll learn

- Why an entity stores Kafka provenance alongside the business fields
- How partition and offset become an idempotency key
- Why the write must happen before the acknowledgment
- Where Lombok belongs, and why the DTO and the entity use different tools

---

## Why this matters

Lesson 17 established that delivery is at-least-once and that the fix is idempotent processing. This is where you build the thing that has to be idempotent.

It is also where the consumer stops being a log line and starts being a system with state, which means every decision here has a consequence you will live with: what identity a record has, what happens when it arrives twice, and what happens when the database is down.

---

## Before you start

[Lesson 17](17-manual-acknowledgment.md), with manual acknowledgment configured.

---

## The concept

### Provenance columns

Alongside the fields from the event itself, the entity stores three facts about the record that carried it:

```java
private int kafkaPartition;
private long kafkaOffset;
private LocalDateTime processedAt;
```

These pay for themselves three times over.

**Debugging.** A row looks wrong. Which record produced it? With the partition and offset you can go and read that exact record from the topic. Without them, the row and the record are unrelated facts.

**Latency measurement.** `processedAt` minus the event's own timestamp gives end-to-end latency per record. A distribution of that is worth more than any dashboard average.

**Idempotency.** Partition and offset together uniquely identify a record in a topic, permanently, because offsets are never reused within a partition. It is a natural, stable, producer-independent deduplication key that costs nothing to compute.

That last one is the one this lesson turns on.

### The idempotency key you already have

From Lesson 17: a crash between the database write and the offset commit causes redelivery, and the listener writes the same row twice. To make that a no-op, you have to recognise the second write as a repeat.

You could deduplicate on a business key, but Wikimedia gives you no stable event identifier that you control, and two genuinely distinct edits to the same page in the same second would collide.

Partition and offset have neither problem:

```java
@Table(name = "wikimedia_events",
       uniqueConstraints = @UniqueConstraint(
               name = "uk_kafka_record",
               columnNames = {"kafka_partition", "kafka_offset"}))
```

Now the second insert violates the constraint. You catch that, treat it as success, and acknowledge. The duplicate is absorbed.

This single constraint is what separates at-least-once from effectively-once. Without it, your table quietly accumulates duplicates every time a consumer restarts at the wrong moment, and every count you compute from it is wrong by an amount nobody can measure.

There is one subtlety. The constraint protects you across restarts and rebalances, which is what you need, but it makes the second write fail rather than succeed. Absorbing that failure is your job, and step 5 shows the shape.

### Acknowledge after the write, never before

```java
repository.save(event);
acknowledgment.acknowledge();
```

If `save()` throws, because the database is down, a constraint failed, or the connection pool is exhausted, the exception propagates out of the listener and `acknowledge()` never runs. The offset stays where it was and Kafka redelivers.

That ordering gives you one guarantee worth stating plainly: no record is marked as processed unless it was actually persisted.

Reverse the two lines and a database outage becomes permanent silent data loss. Every record during the outage is committed as processed and none of them were written anywhere.

### Where Lombok belongs

The convention in this course is Lombok on entities and data-carrying types, never on DTOs, and never on services or controllers.

An entity is the one place it genuinely earns its keep. JPA requires a no-argument constructor and requires mutability, because Hibernate populates fields reflectively when loading a row. Twelve fields means twelve getters and twelve setters of pure ceremony.

Contrast that with `WikimediaEventDto`, which is a record: immutable, no setters, no no-argument constructor. The DTO is data crossing a boundary and the entity is a mutable row managed by a framework. Different tools, for a real reason.

`@Builder` matters here because a twelve-argument constructor call is unreadable and easy to get wrong. Swap two `String` parameters and it still compiles.

### `ddl-auto`, and where this course takes a shortcut

```yaml
jpa:
  hibernate:
    ddl-auto: create-drop
```

`create-drop` builds the schema at startup and destroys it at shutdown. That is convenient for a lesson and unacceptable in production, where your data would disappear on restart and Hibernate would own your schema.

The production posture is `ddl-auto: validate` with Flyway owning migrations under `src/main/resources/db/migration/`. Hibernate then verifies that the schema matches the entities and refuses to start when it does not, so schema changes are reviewed SQL files rather than a side effect of renaming a field.

This course uses an in-memory H2 database and `create-drop` so that the lessons do not require a running PostgreSQL. That is a teaching shortcut, and Lesson 27 lists it as one.

---

## Hands-on

### 1. Add JPA, H2 and Lombok

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>

        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>runtime</scope>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
```

Lombok is an annotation processor, so the compiler plugin needs to know about it, and the Boot plugin should leave it out of the packaged jar:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
```

### 2. Add the datasource

Append to `application.yml`, keeping everything already there:

```yaml
  datasource:
    url: jdbc:h2:mem:wikimediadb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password: password

  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: false

  h2:
    console:
      enabled: true
      path: /h2-console
```

`show-sql: false` is deliberate. At Wikimedia's event rate, logging every statement makes the logs unreadable and the logging itself becomes a bottleneck.

The H2 console is enabled so you can look at the table in a browser, which is genuinely useful in the next few lessons. It is also the kind of thing that must never be exposed in production, and Lesson 27 says so.

### 3. `entity/WikimediaEvent.java`

```java
package com.example.wikimedia.consumer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "wikimedia_events",
        indexes = {
                @Index(name = "idx_wiki", columnList = "wiki"),
                @Index(name = "idx_event_type", columnList = "event_type"),
                @Index(name = "idx_processed_at", columnList = "processed_at")
        },
        uniqueConstraints = @UniqueConstraint(
                name = "uk_kafka_record",
                columnNames = {"kafka_partition", "kafka_offset"}))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WikimediaEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_type", nullable = false)
    private String type;

    @Column(nullable = false)
    private String title;

    // "user" is a reserved SQL keyword, so the column is named "editor".
    @Column(name = "editor")
    private String username;

    private boolean bot;

    private Integer namespace;

    private String wiki;

    @Column(name = "server_name")
    private String serverName;

    @Column(name = "event_timestamp")
    private Long eventTimestamp;

    @Column(columnDefinition = "TEXT")
    private String comment;

    // Kafka provenance: debugging, latency measurement and idempotency.
    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
```

Three details that will bite you if you skip them.

**`user` is a reserved SQL keyword.** The DTO field is `user` and the entity field is `username`, mapped to a column called `editor`. Name a column `user` and your insert fails with a syntax error that points at the wrong part of the statement.

**`comment` needs `columnDefinition = "TEXT"`.** Edit summaries routinely exceed the default 255 characters, and the failure is a truncation error under load.

**The unique constraint is on the column names, not the field names.** `kafka_partition` and `kafka_offset` are what the columns are called; getting this wrong produces a constraint on nothing.

### 4. `repository/WikimediaEventRepository.java`

```java
package com.example.wikimedia.consumer.repository;

import com.example.wikimedia.consumer.entity.WikimediaEvent;
import org.springframework.data.jpa.repository.ListCrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface WikimediaEventRepository
        extends ListCrudRepository<WikimediaEvent, Long>,
                PagingAndSortingRepository<WikimediaEvent, Long> {
}
```

An interface with no implementation and no `@Repository` annotation, because Spring Data generates the proxy.

`ListCrudRepository` rather than `CrudRepository` returns `List` instead of `Iterable` from its finder methods, which is almost always what you actually want. Paging comes from the second interface, and Lesson 23 uses it.

### 5. Persist, then acknowledge

```java
package com.example.wikimedia.consumer.kafka;

import com.example.wikimedia.consumer.dto.WikimediaEventDto;
import com.example.wikimedia.consumer.entity.WikimediaEvent;
import com.example.wikimedia.consumer.repository.WikimediaEventRepository;
import java.time.LocalDateTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class WikimediaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaConsumer.class);

    private final ObjectMapper objectMapper;
    private final WikimediaEventRepository repository;

    public WikimediaConsumer(ObjectMapper objectMapper, WikimediaEventRepository repository) {
        this.objectMapper = objectMapper;
        this.repository = repository;
    }

    @KafkaListener(
            topics = "wikimedia-stream",
            groupId = "wikimedia-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        WikimediaEventDto event = parse(record);

        try {
            repository.save(toEntity(event, record));
        } catch (DataIntegrityViolationException e) {
            // The unique constraint on (kafka_partition, kafka_offset) rejected a
            // record we have already stored. That is redelivery, not a failure:
            // the desired state already exists, so treat it as success.
            log.debug("Already stored partition={} offset={}, skipping",
                    record.partition(), record.offset());
        }

        acknowledgment.acknowledge();
    }

    private WikimediaEvent toEntity(WikimediaEventDto event, ConsumerRecord<String, String> record) {
        return WikimediaEvent.builder()
                .type(event.type())
                .title(event.title())
                .username(event.user())
                .bot(event.bot())
                .namespace(event.namespace())
                .wiki(event.wiki())
                .serverName(event.serverName())
                .eventTimestamp(event.timestamp())
                .comment(event.comment())
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .processedAt(LocalDateTime.now())
                .build();
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

The `catch` block is the idempotency, and it is worth being precise about what it claims.

Catching `DataIntegrityViolationException` and continuing is only correct because the constraint that fired identifies *this exact record*. If the table had other unique constraints, this block would also swallow genuine data problems, and a narrower check would be needed: attempt a lookup by partition and offset first, or inspect the constraint name on the exception.

That is a real limitation rather than a detail. Broad exception catching that happens to be correct today is one schema change away from hiding a bug.

### 6. Run it

Start the consumer, then the producer, and trigger the stream:

```bash
curl http://localhost:8081/api/v1/wikimedia
```

Then look at the table. Browse to `http://localhost:8082/h2-console`, use JDBC URL `jdbc:h2:mem:wikimediadb`, user `sa`, password `password`, and run:

```sql
SELECT COUNT(*) FROM wikimedia_events;
SELECT kafka_partition, COUNT(*) FROM wikimedia_events GROUP BY kafka_partition;
SELECT event_type, COUNT(*) FROM wikimedia_events GROUP BY event_type;
```

The partition counts should roughly reflect the key distribution from Lesson 14, and the event types should show the four kinds the feed emits.

### 7. Prove the idempotency

Stop the consumer. Reset its group back by twenty records, so it is guaranteed to reprocess:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group --topic wikimedia-stream \
  --reset-offsets --shift-by -20 --execute
```

Note the row count first, then restart the consumer with debug logging for your package and watch for the "Already stored" messages.

The row count does not increase by twenty. The records were redelivered, the inserts were rejected by the constraint, and the listener treated each rejection as success.

Now remove the `uniqueConstraints` from the entity, restart to rebuild the schema, and repeat. This time the count grows, and you have twenty duplicate rows that nothing will ever clean up.

Put the constraint back. That comparison is the whole lesson: one line of schema is the difference between at-least-once and effectively-once.

---

## Try it yourself

1. Stop the database mid-stream by shutting down the consumer while the producer runs, then restart it. Which records were lost? Explain using the ordering of `save()` and `acknowledge()`.

2. Swap the two lines so that `acknowledge()` comes first, then throw an exception from `save()` deliberately. How many records are silently lost, and what would you see in Kafka's consumer-group output that would suggest everything is fine?

3. Compute end-to-end latency: `SELECT AVG(DATEDIFF('MILLISECOND', DATEADD('SECOND', event_timestamp, DATE '1970-01-01'), processed_at)) FROM wikimedia_events`. What does that number actually measure, and which parts of the pipeline are inside it?

4. The `catch (DataIntegrityViolationException e)` block is broader than the case it is meant to handle. Add a second unique constraint to the entity, on `title` for example, and show that a genuine violation is now silently swallowed. Then implement a narrower version and say what it costs.

---

## Common mistakes

**Acknowledging before persisting.**
A database outage becomes permanent silent data loss, and the consumer group reports zero lag throughout.

**Omitting the unique constraint.**
Every restart at an awkward moment leaves duplicate rows that nothing detects. Aggregates computed from the table are wrong by an unknowable amount.

**Naming a column `user`.**
A reserved keyword in most SQL dialects. The error message rarely points at the real cause.

**Leaving `comment` as the default varchar.**
Real edit summaries exceed 255 characters, and the failure arrives under load rather than in testing.

**Using Lombok `@Data` on an entity.**
It generates `equals` and `hashCode` across all fields, which interacts badly with JPA identity and lazy loading. Prefer explicit `@Getter` and `@Setter`.

**Leaving `show-sql: true` on a high-throughput consumer.**
The logging becomes the bottleneck and the logs become useless.

**Treating `create-drop` as acceptable beyond a demo.**
Hibernate owning the schema means a renamed field is a migration nobody reviewed.

---

## Check your understanding

**1. Why is partition and offset a better idempotency key than anything in the event payload?**

<details>
<summary>Reveal answer</summary>

Because it is unique, stable, and yours.

Offsets are never reused within a partition, so the pair identifies exactly one record in the topic forever. It does not depend on the publisher providing an identifier, and it cannot collide the way a business key can. Two genuinely different edits to the same page in the same second are indistinguishable by payload and trivially distinguishable by offset.

The one thing it does not survive is republishing the same event to a different offset, for example when replaying from a backup into a new topic. Then the same logical event has a new identity and would be stored twice. If that matters, you need a publisher-provided identifier as well.

</details>

**2. `save()` throws because the database is down. What happens to the record?**

<details>
<summary>Reveal answer</summary>

The exception propagates out of the listener, `acknowledge()` is never reached, the offset is not committed, and Kafka redelivers the record.

Right now that redelivery is immediate and unbounded, so a database outage becomes a tight retry loop against a dead database, which is its own problem. Lesson 20 adds backoff, and Lesson 21 adds somewhere for records to go when retrying will not help.

The important property is already correct though: nothing was marked processed that was not persisted.

</details>

**3. You reverse the two lines, acknowledging first. What does the consumer-group output show during a database outage?**

<details>
<summary>Reveal answer</summary>

Zero lag, and a steadily advancing offset. Everything looks healthy.

That is the trap. Lag measures the distance between the log end and the committed offset, and by committing first you have told Kafka you processed records that you did not. The pipeline reports success while the database receives nothing.

You would find out from the absence of rows, which is a much later and much more expensive discovery than an exception would have been. It is the concrete version of Lesson 17's point that ordering is not a style choice.

</details>

**4. Why does the entity use Lombok while the DTO is a record?**

<details>
<summary>Reveal answer</summary>

Because they have opposite requirements, imposed by different frameworks.

JPA needs a no-argument constructor and mutable fields, because Hibernate instantiates the entity and populates it reflectively when loading a row. A record cannot provide either. So the entity is a mutable class, and Lombok removes twenty-four lines of getter and setter ceremony from it.

The DTO has the opposite need. It is data crossing a boundary, it should never change after construction, and Jackson binds to a record's canonical constructor natively. Adding Lombok there would give you setters on something that must not be mutated.

Using the same tool for both would mean making one of them worse.

</details>

**5. The `catch (DataIntegrityViolationException e)` block treats a constraint violation as success. When is that wrong?**

<details>
<summary>Reveal answer</summary>

As soon as the table has any other constraint that could fire.

Today the only unique constraint is on partition and offset, so a violation can only mean redelivery. Add a unique constraint on anything else, or a non-null constraint that your mapping can breach, and the same catch block will silently discard genuine failures. The record would be acknowledged, nothing would be stored, and no error would be raised anywhere.

The narrower alternatives are to check for existence by partition and offset before saving, which costs a query per record, or to inspect the exception's constraint name and rethrow anything unexpected, which is more precise but couples you to the database's naming.

The general point is that catching a broad exception because it currently only has one cause is a decision with an expiry date, and the expiry is the next schema change.

</details>

---

## Recap

The entity stores the event's fields plus three facts about the record that carried it, and one of those pairs doubles as an idempotency key. A unique constraint on partition and offset turns a redelivered record into a rejected insert, which the listener absorbs as success.

Persist, then acknowledge. That ordering is the difference between a database outage that Kafka retries and one that silently loses everything.

Lombok belongs on the entity and not the DTO, because JPA and Jackson want opposite things.

Your consumer now stores what it reads. Next, more than one thread does it at once.

**Next:** [Lesson 19: Concurrency and Rebalancing](19-concurrency-and-rebalancing.md)
