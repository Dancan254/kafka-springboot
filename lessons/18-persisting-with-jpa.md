# Lesson 18 — Persisting with JPA

> **Part 3 — The Consumer** · 25 minutes

---

## What you'll learn

- Why a consumer should store Kafka provenance columns alongside the data
- How `(partition, offset)` becomes an idempotency key
- Where Lombok belongs, and why an entity is the one place it earns its keep
- Why "ack after the write" is the sentence the whole pipeline rests on

---

## Why this matters

Lesson 17 ended with a promise: at-least-once delivery is safe *if* processing is idempotent. This is where you make good on it.

The consumer is also about to gain a real side effect. Until now, failure meant a log line. From here on, failure means a half-written database — and the interaction between the transaction, the exception, and the offset commit is where subtle bugs live.

---

## Before you start

[Lesson 17](17-manual-acknowledgment.md). Manual ack configured and working.

---

## The concept

### Store where the record came from

A naive schema stores the nine fields from the DTO. A useful one also stores three more:

```java
private int kafkaPartition;
private long kafkaOffset;
private LocalDateTime processedAt;
```

These are **provenance** columns, and they pay for themselves three times over.

**Debugging.** A row looks wrong. Which record produced it? `(partition=1, offset=8423)` — go read it with `kafka-console-consumer --partition 1 --offset 8423`. Without these columns, the row and the record are unrelated facts.

**Lag investigation.** `processedAt` minus the event's own `timestamp` tells you end-to-end latency, per record. A histogram of that is worth more than any dashboard.

**Idempotency.** `(partition, offset)` uniquely identifies a record in a topic, forever. Offsets are never reused within a partition. It is a natural, stable, producer-independent dedup key that costs you nothing to compute.

That last one is the important one.

### The idempotency key you already have

From Lesson 17: a crash between the database write and the offset commit causes redelivery, and the listener writes the same row twice. To make that a no-op you need to recognise the second write as a repeat.

You could dedup on a business key — but Wikimedia has no stable event ID you control, and two genuinely distinct edits to the same page in the same second would collide.

`(kafkaPartition, kafkaOffset)` has none of those problems. Add a unique constraint:

```java
@Table(name = "wikimedia_events",
       uniqueConstraints = @UniqueConstraint(columnNames = {"kafka_partition", "kafka_offset"}))
```

Now the second insert violates the constraint. You catch it, treat it as success, and acknowledge. The duplicate is absorbed.

> The reference implementation in this repository indexes those columns but does **not** add the unique constraint. It therefore stores duplicates on redelivery. The exercise asks you to fix that — and it's worth understanding that this is the single line separating "at-least-once" from "effectively-once."

### Ack after the write, not before

```java
repository.save(event);   // durable
ack.acknowledge();        // now, and only now
```

If `save()` throws — the database is down, a constraint fails, the connection pool is exhausted — the exception propagates out of the listener. `acknowledge()` never runs. The offset stays put. Kafka will redeliver.

That single ordering gives you: **no record is ever marked processed unless it was actually persisted.**

Reverse the two lines and a database outage becomes permanent, silent data loss. Every record during the outage gets committed as processed, and nothing wrote them anywhere.

### Where Lombok belongs

The house rule: Lombok on entities and data-carrying POJOs; never on DTOs; never on services or controllers.

An entity is the one place it genuinely earns its keep. JPA *requires* a no-args constructor. It *requires* mutability, because Hibernate populates fields reflectively on load. Twelve fields means twelve getters and twelve setters of pure ceremony.

```java
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class WikimediaEvent { ... }
```

Contrast with `WikimediaEventDto`, which is a `record`: immutable, no setters, no no-args constructor. The DTO is data crossing a boundary; the entity is a mutable row managed by a framework. Different tools, for a real reason.

`@Builder` matters here because a twelve-argument constructor call is unreadable and easy to get wrong — swap two `String` parameters and it still compiles.

### `ddl-auto` and why this project cheats

```yaml
jpa:
  hibernate:
    ddl-auto: create-drop
```

`create-drop` builds the schema at startup and destroys it at shutdown. Convenient for a demo, unacceptable in production: your data disappears on restart, and Hibernate owns your schema.

The production posture is `ddl-auto: validate` with **Flyway** owning migrations in `src/main/resources/db/migration/`. Hibernate then verifies the schema matches the entities and refuses to start if it doesn't. Schema changes are reviewed SQL files, not a side effect of renaming a field.

This project uses an in-memory H2 database and `create-drop` because a lesson repo shouldn't require a running Postgres. Be clear that it's a teaching shortcut.

---

## Hands-on

### 1. Add JPA and H2

`pom.xml`:

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

Lombok needs the annotation processor wired into the compiler plugin:

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
```

### 2. Datasource config

Append to `application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:wikimediadb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password: password

  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      # Teaching shortcut. Production: validate, with Flyway owning migrations.
      ddl-auto: create-drop
    show-sql: false

  h2:
    console:
      enabled: true
      path: /h2-console
```

`DB_CLOSE_DELAY=-1` keeps the in-memory database alive after the last connection closes. Without it, H2 discards your schema the moment the pool goes idle.

### 3. `entity/WikimediaEvent.java`

```java
package com.javaguy.consumer.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "wikimedia_events", indexes = {
        @Index(name = "idx_wiki", columnList = "wiki"),
        @Index(name = "idx_event_type", columnList = "event_type"),
        @Index(name = "idx_processed_at", columnList = "processed_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WikimediaEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // "edit", "new", "log", "categorize"
    @Column(name = "event_type", nullable = false)
    private String type;

    @Column(nullable = false)
    private String title;

    // "user" is a reserved SQL keyword — stored as "editor"
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

    // Kafka provenance — debugging, latency measurement, and idempotency.
    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
```

Two details that will bite you if you skip them:

**`user` is a reserved SQL keyword.** The DTO field is `user`; the entity field is `username`, mapped to a column called `editor`. Name a column `user` and your `INSERT` fails on most databases with a syntax error that names the wrong line.

**`comment` needs `columnDefinition = "TEXT"`.** Edit summaries exceed the default `VARCHAR(255)` regularly, and the failure is a truncation error at 3 a.m.

### 4. `repository/WikimediaEventRepository.java`

```java
package com.javaguy.consumer.repository;

import com.javaguy.consumer.entity.WikimediaEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaEventRepository extends JpaRepository<WikimediaEvent, Long> {
}
```

An interface. No implementation, no `@Repository` annotation needed — Spring Data generates the proxy. Query methods come in Lesson 23.

### 5. Persist, then acknowledge

```java
package com.javaguy.consumer.consumer;

import com.javaguy.consumer.dto.WikimediaEventDto;
import com.javaguy.consumer.entity.WikimediaEvent;
import com.javaguy.consumer.repository.WikimediaEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Service
public class WikimediaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaConsumer.class);

    private final WikimediaEventRepository repository;
    private final ObjectMapper objectMapper;

    public WikimediaConsumer(WikimediaEventRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "wikimedia-stream",
            groupId = "wikimedia-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        WikimediaEventDto dto = parse(record);

        WikimediaEvent event = WikimediaEvent.builder()
                .type(dto.type())
                .title(dto.title())
                .username(dto.user())
                .bot(dto.bot())
                .namespace(dto.namespace())
                .wiki(dto.wiki())
                .serverName(dto.serverName())
                .eventTimestamp(dto.timestamp())
                .comment(dto.comment())
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .processedAt(LocalDateTime.now())
                .build();

        repository.save(event);

        log.info("Saved | partition={} offset={} type={} wiki={} title='{}'",
                record.partition(), record.offset(), dto.type(), dto.wiki(), dto.title());

        // Commit the offset only after the record is durably in the database.
        // If save() throws, this never runs, the offset stays put, and Kafka redelivers.
        acknowledgment.acknowledge();
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

### 6. Run it

Start the consumer, then the producer, then trigger the stream:

```bash
curl http://localhost:8081/api/v1/wikimedia
```

```
Saved | partition=0 offset=483 type=edit wiki=commonswiki title='File:Prigioni cella.JPG'
Saved | partition=2 offset=257 type=edit wiki=wikidatawiki title='Q15440113'
```

### 7. Look at the rows

Open **http://localhost:8082/h2-console**.

- JDBC URL: `jdbc:h2:mem:wikimediadb`
- User: `sa`
- Password: `password`

```sql
SELECT COUNT(*) FROM wikimedia_events;

SELECT kafka_partition, kafka_offset, event_type, wiki, title, processed_at
FROM wikimedia_events
ORDER BY processed_at DESC
LIMIT 10;

-- End-to-end latency, in seconds, per record
SELECT AVG(DATEDIFF('SECOND',
       DATEADD('SECOND', event_timestamp, DATE '1970-01-01'),
       processed_at)) AS avg_latency_seconds
FROM wikimedia_events;
```

That last query is why `processedAt` and `eventTimestamp` are both stored.

### 8. Prove the duplicate exists

Stop the consumer. Reset the group back a few records:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --group wikimedia-consumer-group --topic wikimedia-stream \
  --reset-offsets --shift-by -5 --execute
```

Restart the consumer and query:

```sql
SELECT kafka_partition, kafka_offset, COUNT(*)
FROM wikimedia_events
GROUP BY kafka_partition, kafka_offset
HAVING COUNT(*) > 1;
```

Rows come back. The same Kafka record is now in the database twice.

This is at-least-once delivery, doing exactly what it promises. Nothing is misconfigured. The fix is the unique constraint in the exercise below — and once you add it, the second `save()` throws `DataIntegrityViolationException`, which you catch and treat as success.

---

## Try it yourself

1. Add `uniqueConstraints = @UniqueConstraint(columnNames = {"kafka_partition", "kafka_offset"})` to `@Table`. Catch `DataIntegrityViolationException` around `save()`, log at debug, and acknowledge anyway. Repeat the offset reset from step 8. How many rows now? You have turned at-least-once into effectively-once.

2. Stop the H2 database mid-stream — set the JDBC URL to something invalid and restart the consumer with records pending. Does the offset advance? Where do the records go? (They're still in Kafka. Nothing is lost. That's the point.)

3. Move `acknowledge()` to *before* `repository.save(event)`. Reset offsets, run, and kill the consumer while records are flowing. Compare the row count to the committed offset. Quantify the loss.

4. Swap `create-drop` for `validate` and add a Flyway migration under `src/main/resources/db/migration/V1__create_wikimedia_events.sql`. Restart. What does Hibernate do if your SQL and your entity disagree?

---

## Common mistakes

**Acknowledging before persisting.**
A database outage becomes permanent data loss, and the offsets claim everything succeeded.

**Acknowledging in a `finally` block.**
Commits on failure too. That's auto-commit with more typing.

**Naming a column `user`.**
Reserved SQL keyword. Use `editor`, or quote it forever.

**Leaving `comment` as the default `VARCHAR(255)`.**
Wikimedia edit summaries are longer than that, routinely.

**Relying on `ddl-auto: create-drop` beyond a demo.**
Your data vanishes on restart, and Hibernate owns your schema. Use `validate` plus Flyway.

**Storing no Kafka provenance.**
You lose your idempotency key, your debugging path back to the record, and your latency measurement — all for three columns.

**Assuming `repository.save()` is transactional with the offset commit.**
It isn't, and it can't be. Two systems, no shared transaction. That gap is why idempotency is mandatory rather than optional.

---

## Check your understanding

**1. Why is `(kafkaPartition, kafkaOffset)` a better idempotency key than a hash of the event payload?**

<details>
<summary>Reveal answer</summary>

Because it's unique per *record*, not per *content*, and it's stable forever.

Two genuinely distinct edits could produce byte-identical payloads — the same user reverting the same page twice in the same second, say. A content hash would treat the second as a duplicate and silently drop a real event.

`(partition, offset)` identifies a physical position in the log. Offsets are assigned by the broker, increase monotonically, and are never reused within a partition. Redelivery of the same record always presents the same pair; two different records never share one.

It's also free — you already have both values on the `ConsumerRecord`, no hashing required.

</details>

**2. `repository.save()` succeeds, then the JVM is killed before `acknowledge()`. What happens on restart, and is anything wrong?**

<details>
<summary>Reveal answer</summary>

The record is redelivered and `save()` runs again.

Nothing is wrong — this is the irreducible at-least-once window. The database write and the Kafka offset commit are separate systems with no shared transaction, so there is always a moment where one has happened and the other hasn't.

Without a unique constraint you get a duplicate row. With one, the second `save()` throws `DataIntegrityViolationException`, you catch it, acknowledge, and move on. The duplicate becomes a no-op.

Reversing the two lines wouldn't help; it would convert this duplicate into permanent data loss.

</details>

**3. Your database is down for ten minutes. `save()` throws on every record. What is the state of the topic, the offsets, and your data when it recovers?**

<details>
<summary>Reveal answer</summary>

The topic is untouched — records keep arriving from the producer and are retained normally.

Offsets do not advance, because `acknowledge()` is never reached. `CURRENT-OFFSET` stays frozen at wherever the consumer was when the database died, and lag climbs steadily.

When the database recovers, the consumer resumes from that exact offset and processes the ten-minute backlog. **No records are lost.**

This is the whole payoff of "ack after the write." A downstream outage becomes lag — a recoverable, visible, alertable condition — instead of data loss. Kafka absorbed the outage as a buffer.

The one deadline: if the outage lasted longer than the topic's retention (7 days here), the unread records would be deleted and lost.

</details>

**4. The house rule is "no Lombok on DTOs, Lombok on entities." Given both are data classes, what's the actual distinction?**

<details>
<summary>Reveal answer</summary>

Mutability and framework requirements, not "data-ness."

A DTO is an immutable value crossing a boundary. A `record` gives you exactly that — final fields, no setters, a canonical constructor, correct `equals`. Lombok's `@Data` would give you a *mutable* class with setters, which is strictly worse for an event you never intend to modify.

A JPA entity cannot be a record. Hibernate needs a no-args constructor to instantiate it and setters (or field access) to populate it on load, and it tracks changes by mutating the managed instance. Immutability is off the table. Given twelve mutable fields, Lombok removes twenty-four methods of pure ceremony, and `@Builder` makes the twelve-argument construction readable and type-safe.

So the rule isn't stylistic. Use records where immutability is possible; use Lombok where a framework forces mutability on you.

</details>

**5. You reverse the two lines — `acknowledge()` then `save()` — and the database goes down for ten minutes. Compare the outcome to question 3.**

<details>
<summary>Reveal answer</summary>

Every record produced during those ten minutes is **permanently lost**, and Kafka's offsets assert that all of them were processed successfully.

The listener acknowledges first, so the offset advances. Then `save()` throws. The exception propagates, the error handler logs it (or retries a record whose offset has already been committed, which does nothing useful), and the consumer moves to the next record — committing that one too.

On recovery, the consumer resumes from an offset far past the outage. The missing records are still sitting in the topic for another seven days, entirely readable, and nothing in your system knows they were skipped. You would discover it from a downstream count mismatch, weeks later, if at all.

Question 3's version turns the same outage into lag. This one turns it into silent, unbounded data loss. Two lines, swapped.

</details>

---

## Recap

The consumer now writes rows, and it stores `(partition, offset, processedAt)` alongside the data — for debugging, for latency, and above all as a natural idempotency key. `acknowledge()` runs only after `save()` returns, so a database outage becomes lag rather than loss.

Lombok on the entity, a record for the DTO, and a bare interface for the repository.

One consumer thread is currently reading three partitions. Let's give each one its own.

**Next:** [Lesson 19 — Concurrency & rebalancing →](19-concurrency-and-rebalancing.md)
