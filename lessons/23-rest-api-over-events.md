# Lesson 23 — A REST API Over Consumed Events

> **Part 5 — Production** · 25 minutes

---

## What you'll learn

- Why an entity must never be returned from a controller
- How to build response DTOs as records, and where the mapping belongs
- Why `Page<Entity>` is a worse API contract than it looks
- How to write aggregation queries without `List<Object[]>`

---

## Why this matters

You have a database full of Wikimedia edits and no way to read them. Adding a controller is easy. Adding one that doesn't quietly couple your HTTP contract to your JPA schema is the actual lesson.

This is also where the layering rules stop being style advice. A Kafka consumer that leaks its entity to the API means every future migration — renaming a column, adding a lazy association, switching to Avro — becomes a breaking change for clients who were never told they depended on it.

---

## Before you start

[Lesson 22](22-dlt-headers-and-replay.md). A consumer persisting events to H2.

Add the web starter if you haven't:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webmvc</artifactId>
</dependency>
```

Note the name. In Spring Boot 4 this is `spring-boot-starter-webmvc`, not `spring-boot-starter-web`.

---

## The concept

### Entities stop at the service layer

`WikimediaEvent` is a JPA entity. It is a mutable, Hibernate-managed object with an identity, a persistence context, and a schema.

Return it from a controller and four things happen, none of them good:

**Your column names become your JSON field names.** Rename `editor` to `contributor` in a migration and every API consumer breaks.

**You leak internals.** `id`, `kafkaPartition`, `kafkaOffset` are implementation details of *how* you ingested the event. A client asking for Wikipedia edits does not need your Kafka offsets.

**Serialization can trigger queries.** With a lazy association, Jackson touching the getter fires a `SELECT` mid-serialization, outside any transaction. This is where `LazyInitializationException` comes from, and where N+1 queries hide.

**You cannot evolve them independently.** The whole point of a DTO is that the API contract and the storage schema change for different reasons, on different schedules.

So: map to a response DTO before returning. Always.

### Response DTOs are records

```java
public record WikimediaEventResponse(
        String type,
        String title,
        String editor,
        boolean bot,
        String wiki,
        Instant occurredAt
) {}
```

Immutable, no Lombok, no setters. And notice it is *not* a field-for-field copy of the entity: `username` became `editor`, the epoch-seconds `Long` became an `Instant`, and the Kafka provenance columns are gone entirely.

That divergence is the point. If your DTO is a mechanical copy of your entity, you haven't designed an API — you've published your schema with extra steps.

**Separate request and response DTOs.** This lesson has no request bodies, but the rule holds: never reuse one record for both directions. They validate differently and evolve differently.

### Where mapping belongs

Not in the controller. The controller handles HTTP: parse the request, call the service, choose a status code.

Not in the entity either — an entity that knows how to become a DTO knows about the layer above it.

A static factory on the response record is the smallest thing that works:

```java
public record WikimediaEventResponse(...) {
    public static WikimediaEventResponse from(WikimediaEvent event) { ... }
}
```

The record depends on the entity; the entity knows nothing. Dependencies point inward.

> In a larger project this belongs in a dedicated mapper. For a demo, a static factory keeps the indirection down — and the house rule is that a new abstraction needs a reason.

### `Page<T>` is a leaky contract

`Page<WikimediaEvent>` serialises to JSON containing `content`, `pageable`, `sort`, `numberOfElements`, `first`, `last`, `empty` — Spring Data's internal pagination model, published as your API.

It has changed shape across Spring Data versions, and it warns about exactly this at runtime in recent versions. Map to `Page<WikimediaEventResponse>` at minimum; better, return your own small envelope so pagination metadata is a contract you own.

For this lesson `Page<WikimediaEventResponse>` is enough — the DTO mapping is the point.

### Aggregations without `Object[]`

The obvious way to count events per wiki:

```java
@Query("SELECT e.wiki, COUNT(e) FROM WikimediaEvent e GROUP BY e.wiki")
List<Object[]> countGroupedByWiki();
```

Then in the service:

```java
row -> (String) row[0], row -> (Long) row[1]
```

Two unchecked casts, positional indexing, and nothing catches a mistake until runtime. Swap the columns in the query and it compiles, deploys, and throws `ClassCastException` in production.

Spring Data supports **interface projections**, which give the compiler something to check:

```java
public interface WikiCount {
    String getWiki();
    long getCount();
}

@Query("SELECT e.wiki AS wiki, COUNT(e) AS count FROM WikimediaEvent e GROUP BY e.wiki ORDER BY COUNT(e) DESC")
List<WikiCount> countGroupedByWiki();
```

The `AS` aliases must match the getter names. Now the mapping is by name, not position.

---

## Hands-on

### 1. Repository query methods

```java
package com.javaguy.consumer.repository;

import com.javaguy.consumer.entity.WikimediaEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface WikimediaEventRepository extends JpaRepository<WikimediaEvent, Long> {

    Page<WikimediaEvent> findByWiki(String wiki, Pageable pageable);

    Page<WikimediaEvent> findByType(String type, Pageable pageable);

    List<WikimediaEvent> findTop10ByOrderByProcessedAtDesc();

    long countByBot(boolean bot);

    @Query("""
            SELECT e.wiki AS name, COUNT(e) AS total
            FROM WikimediaEvent e
            GROUP BY e.wiki
            ORDER BY COUNT(e) DESC
            """)
    List<NameCount> countGroupedByWiki();

    @Query("""
            SELECT e.type AS name, COUNT(e) AS total
            FROM WikimediaEvent e
            GROUP BY e.type
            ORDER BY COUNT(e) DESC
            """)
    List<NameCount> countGroupedByType();

    /** Interface projection — Spring Data maps by alias name, not column position. */
    interface NameCount {
        String getName();
        long getTotal();
    }
}
```

The derived query names do real work. `findTop10ByOrderByProcessedAtDesc` needs no `@Query` at all — Spring Data parses the method name into `SELECT ... ORDER BY processed_at DESC LIMIT 10`. That's why the naming convention is worth learning.

Text blocks for the JPQL, because a one-line `@Query` string with a `GROUP BY` is unreadable.

### 2. Response DTOs

`dto/WikimediaEventResponse.java`:

```java
package com.javaguy.consumer.dto;

import com.javaguy.consumer.entity.WikimediaEvent;

import java.time.Instant;

public record WikimediaEventResponse(
        String type,
        String title,
        String editor,
        boolean bot,
        String wiki,
        String serverName,
        Instant occurredAt,
        String comment
) {
    public static WikimediaEventResponse from(WikimediaEvent event) {
        return new WikimediaEventResponse(
                event.getType(),
                event.getTitle(),
                event.getUsername(),
                event.isBot(),
                event.getWiki(),
                event.getServerName(),
                event.getEventTimestamp() == null ? null : Instant.ofEpochSecond(event.getEventTimestamp()),
                event.getComment()
        );
    }
}
```

`id`, `kafkaPartition`, `kafkaOffset`, and `processedAt` are deliberately absent. They are how the record got here, not what it is.

`dto/EventStatsResponse.java`:

```java
package com.javaguy.consumer.dto;

import java.util.Map;

public record EventStatsResponse(
        long totalEvents,
        long botEdits,
        long humanEdits,
        Map<String, Long> byWiki,
        Map<String, Long> byType
) {}
```

A record, not `Map<String, Object>`. The response shape is now checked by the compiler and visible in one place.

### 3. The controller

```java
package com.javaguy.consumer.controller;

import com.javaguy.consumer.dto.EventStatsResponse;
import com.javaguy.consumer.dto.WikimediaEventResponse;
import com.javaguy.consumer.service.WikimediaEventService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/wikimedia/events")
public class WikimediaEventController {

    private final WikimediaEventService service;

    public WikimediaEventController(WikimediaEventService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<Page<WikimediaEventResponse>> getAll(
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return ResponseEntity.ok(service.findAll(pageable));
    }

    @GetMapping("/recent")
    public ResponseEntity<List<WikimediaEventResponse>> getRecent() {
        return ResponseEntity.ok(service.findRecent());
    }

    @GetMapping("/wiki/{wiki}")
    public ResponseEntity<Page<WikimediaEventResponse>> getByWiki(
            @PathVariable String wiki,
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return ResponseEntity.ok(service.findByWiki(wiki, pageable));
    }

    @GetMapping("/type/{type}")
    public ResponseEntity<Page<WikimediaEventResponse>> getByType(
            @PathVariable String type,
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return ResponseEntity.ok(service.findByType(type, pageable));
    }

    @GetMapping("/stats")
    public ResponseEntity<EventStatsResponse> getStats() {
        return ResponseEntity.ok(service.computeStats());
    }
}
```

No Lombok. No repository. No mapping. No try/catch. Explicit `ResponseEntity` with an explicit status. The controller's entire job is HTTP.

### 4. The service

```java
package com.javaguy.consumer.service;

import com.javaguy.consumer.dto.EventStatsResponse;
import com.javaguy.consumer.dto.WikimediaEventResponse;
import com.javaguy.consumer.repository.WikimediaEventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Transactional(readOnly = true)
public class WikimediaEventService {

    private final WikimediaEventRepository repository;

    public WikimediaEventService(WikimediaEventRepository repository) {
        this.repository = repository;
    }

    public Page<WikimediaEventResponse> findAll(Pageable pageable) {
        return repository.findAll(pageable).map(WikimediaEventResponse::from);
    }

    public List<WikimediaEventResponse> findRecent() {
        return repository.findTop10ByOrderByProcessedAtDesc().stream()
                .map(WikimediaEventResponse::from)
                .toList();
    }

    public Page<WikimediaEventResponse> findByWiki(String wiki, Pageable pageable) {
        return repository.findByWiki(wiki, pageable).map(WikimediaEventResponse::from);
    }

    public Page<WikimediaEventResponse> findByType(String type, Pageable pageable) {
        return repository.findByType(type, pageable).map(WikimediaEventResponse::from);
    }

    public EventStatsResponse computeStats() {
        long total = repository.count();
        long botEdits = repository.countByBot(true);

        return new EventStatsResponse(
                total,
                botEdits,
                total - botEdits,
                toOrderedMap(repository.countGroupedByWiki()),
                toOrderedMap(repository.countGroupedByType())
        );
    }

    // LinkedHashMap preserves the query's ORDER BY COUNT DESC; toMap() would not.
    private Map<String, Long> toOrderedMap(List<WikimediaEventRepository.NameCount> rows) {
        return rows.stream().collect(Collectors.toMap(
                WikimediaEventRepository.NameCount::getName,
                WikimediaEventRepository.NameCount::getTotal,
                (a, b) -> a,
                LinkedHashMap::new));
    }
}
```

`Page.map()` does the DTO conversion while preserving pagination metadata — no manual page arithmetic.

`@Transactional(readOnly = true)` at the class level. It gives Hibernate permission to skip dirty-checking, and it means `computeStats()`'s three queries see one consistent snapshot rather than three.

That `LinkedHashMap` detail matters. Your JPQL says `ORDER BY COUNT(e) DESC`, and `Collectors.toMap()` returns a `HashMap`, which throws that ordering away. The API would return wikis in hash order while the query carefully sorted them. It's the kind of bug that survives code review because both lines look right.

### 5. Run it

```bash
curl -s 'http://localhost:8082/api/v1/wikimedia/events/recent' | jq '.[0]'
```

```json
{
  "type": "edit",
  "title": "Nikola Tesla",
  "editor": "Some Contributor",
  "bot": false,
  "wiki": "enwiki",
  "serverName": "en.wikipedia.org",
  "occurredAt": "2026-07-10T08:41:33Z",
  "comment": "fixed a typo"
}
```

No `id`. No `kafkaOffset`. An ISO-8601 timestamp instead of epoch seconds.

```bash
curl -s 'http://localhost:8082/api/v1/wikimedia/events/stats' | jq
curl -s 'http://localhost:8082/api/v1/wikimedia/events/wiki/enwiki?size=5' | jq '.content | length'
curl -s 'http://localhost:8082/api/v1/wikimedia/events/type/edit?page=0&size=3' | jq '.totalElements'
```

### 6. Compare with the reference implementation

Open `consumer/src/main/java/com/javaguy/consumer/controller/WikimediaEventController.java` in the repository root.

It returns `Page<WikimediaEvent>` — the entity. It carries `@RequiredArgsConstructor` on a controller. It injects the repository directly, skipping the service layer. Its `/stats` returns `Map<String, Object>`, and it casts `Object[]` rows positionally.

It works. It also publishes the JPA schema as an API contract, and its `/stats` map ordering is at the mercy of `HashMap`.

Both versions are in this repository on purpose. The reference implementation is what a lot of real Kafka demos look like; this lesson is what it should look like.

---

## Try it yourself

1. Rename the entity field `username` to `contributor` (keep `@Column(name = "editor")`). Which version of the controller breaks its API contract — yours or the reference one? Now do the same to the `@Column` name.

2. Add `@ManyToOne(fetch = LAZY)` to the entity. Call the reference controller's `GET /events`. What exception, and why does the DTO version not have this problem?

3. Replace the interface projection with `List<Object[]>` and swap the two `SELECT` columns. Does it compile? When do you find out?

4. Add a `GET /events/{id}` that returns `404` when the id doesn't exist. Do it with `ProblemDetail` (RFC 9457) and a `@ControllerAdvice`, not a `try/catch` in the controller.

---

## Common mistakes

**Returning the entity from a controller.**
Couples your API to your schema, leaks internals, and invites `LazyInitializationException` during serialization.

**Returning `Page<Entity>`.**
Publishes Spring Data's internal pagination model as your contract. It has changed shape between versions.

**`Collectors.toMap()` after an `ORDER BY`.**
`HashMap` discards the ordering you asked the database for. Use `LinkedHashMap`.

**`List<Object[]>` with positional casts.**
Unchecked, position-dependent, and fails at runtime. Use an interface projection or a DTO projection.

**Lombok on a controller or service.**
The house rule. Constructor injection you can see beats one you have to infer.

**Injecting the repository into the controller.**
Skips the service layer. Fine until the second caller needs the same logic.

**Forgetting `@Transactional(readOnly = true)` on multi-query reads.**
`computeStats()` runs four queries; without it, they're four separate transactions and the counts can disagree with each other.

---

## Check your understanding

**1. Your controller returns `WikimediaEvent`. A DBA renames the column `editor` to `contributor` and updates `@Column(name = ...)`. Does the API change?**

<details>
<summary>Reveal answer</summary>

No — and that's the point of the trap.

`@Column(name = "editor")` maps the *database column*. Jackson serialises the **Java field name**, `username`. So renaming the column changes nothing for clients.

But rename the **field** from `username` to `contributor` — a refactor an IDE will do silently across the codebase, keeping `@Column` intact — and every API response changes shape. No compiler error, no failing test unless you have one asserting the JSON.

That's the asymmetry. Your API contract is now hostage to a refactoring operation that looks purely internal. A response DTO makes the contract explicit: renaming the entity field forces you to edit `WikimediaEventResponse.from()`, and the API only changes when you change the record.

</details>

**2. Why is `Collectors.toMap()` wrong here when the query already has `ORDER BY COUNT(e) DESC`?**

<details>
<summary>Reveal answer</summary>

Because `toMap()` returns a `HashMap`, and `HashMap` has no ordering. It arranges entries by hash bucket.

The database did the sorting work — the most active wiki first — and the collector immediately discarded it. The JSON response comes back in an arbitrary order that happens to be stable for a given set of keys, so it looks deterministic in testing and reshuffles the moment the data changes.

`Collectors.toMap(keyFn, valueFn, mergeFn, LinkedHashMap::new)` preserves encounter order, which is the query's order.

This is a good example of two correct-looking lines producing a wrong result. Neither the query nor the collector is buggy; the *composition* is.

</details>

**3. What's the concrete failure when a controller returns an entity with a lazy association?**

<details>
<summary>Reveal answer</summary>

`LazyInitializationException`, thrown by Jackson, during response serialization.

The service method's transaction commits when it returns. The entity leaves the persistence context detached, with its lazy association still an uninitialised proxy. Jackson then walks the object graph to build JSON, calls the getter for that association, and the proxy tries to load it — with no open session to load it from.

The failure happens *after* your controller returned successfully, inside the HTTP message converter. The response has already begun streaming, so the client may get a `200` with truncated JSON.

Mapping to a DTO inside the transactional service method eliminates it entirely: you touch what you need while the session is open, and hand back a plain record with no proxies in it.

</details>

**4. `List<Object[]>` versus an interface projection. Both work. Why does the interface version prevent a class of bug?**

<details>
<summary>Reveal answer</summary>

Because the mapping is by **name**, not by **position**.

With `Object[]`, `row[0]` is whatever the first `SELECT` expression happened to be, cast to whatever you claim it is. Reorder the `SELECT` clause — a harmless-looking edit — and `(String) row[0]` becomes a `ClassCastException` at runtime, in production, on the first request that hits the endpoint.

With `interface NameCount { String getName(); long getTotal(); }` and `SELECT e.wiki AS name, COUNT(e) AS total`, Spring Data binds by alias. Reordering the `SELECT` changes nothing. Removing an alias fails fast at startup when the projection can't be satisfied.

You've moved the failure from "runtime, on user traffic" to "compile time or context startup." That's the whole trade.

</details>

**5. The reference controller in this repo injects the repository directly and returns entities. It has worked in production-style demos for years. What is the actual, concrete cost?**

<details>
<summary>Reveal answer</summary>

The cost is deferred, not absent, and it lands all at once.

The API contract is *implicit* — defined by whatever fields the entity currently has. Nobody wrote it down, no test asserts it, and every client has quietly come to depend on `id` and `kafkaOffset` being present. Now:

- Adding a column adds a field to every response. Probably harmless, occasionally not.
- Renaming a field is a breaking change disguised as a refactor.
- Adding a lazy association breaks serialization.
- Switching the entity to Avro-generated classes (Lesson 25) means the API changes shape.
- You cannot version the API independently of the schema, because they're the same thing.

None of this hurts while the schema is stable and there's one client. All of it hurts on the day you need to change the schema, which is the day you can least afford a coordinated client migration.

The DTO is insurance with a small, constant premium.

</details>

---

## Recap

Entities stop at the service layer. Response DTOs are records, deliberately *not* mirrors of the entity — no `id`, no Kafka offsets, an `Instant` instead of epoch seconds. Controllers return `ResponseEntity` and do nothing but HTTP. Aggregations use interface projections so the compiler checks the mapping, and `LinkedHashMap` so the database's `ORDER BY` survives contact with the collector.

Your pipeline now ingests, persists, and serves. None of it is tested.

**Next:** [Lesson 24 — Testing Kafka with Testcontainers →](24-testing-with-testcontainers.md)
