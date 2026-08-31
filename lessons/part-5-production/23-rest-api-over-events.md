# Lesson 23: A REST API over Consumed Events

> **Part 5: Production**

---

## What you'll learn

- Why a JPA entity must not leave the service layer, with the failure modes named
- How a response DTO differs from a mechanical copy of the entity
- Why `Page<T>` is a leaky API contract
- How interface projections replace `Object[]` and unchecked casts

---

## Why this matters

Your consumer stores events and nothing reads them. Putting an HTTP API over the table is the obvious next step, and it is also the step where a working pipeline acquires a permanent design mistake.

The mistake is easy to make, invisible in testing, and expensive to reverse: returning the entity. This lesson builds the wrong version first so you can see what it publishes, then refactors it.

---

## Before you start

[Lesson 22](../part-4-resilience/22-dlt-headers-and-replay.md), with events in the table.

Add the web starter to the consumer:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc</artifactId>
        </dependency>
```

Note the name. In Spring Boot 4 this is `spring-boot-starter-webmvc`, not `spring-boot-starter-web`. The consumer already had a web server from actuator; this adds Spring MVC to it.

---

## The concept

### Entities stop at the service layer

`WikimediaEvent` is a JPA entity: a mutable, Hibernate-managed object with an identity, a persistence context and a schema.

Return it from a controller and four things happen, none of them good.

**Your column names become your JSON field names.** Rename `editor` to `contributor` in a migration and every API client breaks, for a change that had nothing to do with them.

**You leak internals.** `id`, `kafkaPartition` and `kafkaOffset` are details of how you ingested the event. A client asking for Wikipedia edits does not need your Kafka offsets, and once they can see them somebody will start depending on them.

**Serialization can trigger queries.** With a lazy association, Jackson touching a getter fires a select mid-serialization, outside any transaction. That is where `LazyInitializationException` comes from, and where N+1 queries hide.

**You cannot evolve them independently.** The API contract and the storage schema change for different reasons on different schedules, which is the entire argument for a DTO.

### Response DTOs are records, not copies

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

Immutable, no Lombok, no setters, following the same rule as the inbound DTO in Lesson 16.

Notice it is not a field-for-field copy. `username` became `editor`, the epoch-seconds `Long` became an `Instant`, and the Kafka provenance columns are gone entirely.

That divergence is the point. If your response DTO is a mechanical copy of your entity, you have not designed an API, you have published your schema with extra steps and taken on the mapping cost anyway.

Keep request and response DTOs separate as a rule, even though this lesson has no request bodies. They validate differently and evolve differently.

### Where mapping belongs

Not in the controller, which handles HTTP concerns: parse the request, call the service, choose a status code.

Not in the entity either, because an entity that knows how to become a DTO knows about the layer above it.

A static factory on the response record is the smallest thing that works, and dependencies point the right way: the record depends on the entity, and the entity knows nothing.

In a larger codebase this belongs in a dedicated mapper. For a pipeline this size, a static factory keeps the indirection down.

### `Page<T>` is a leaky contract

`Page<WikimediaEvent>` serialises to JSON containing `content`, `pageable`, `sort`, `numberOfElements`, `first`, `last` and `empty`: Spring Data's internal pagination model, published as your API.

That shape has changed across Spring Data versions, and recent versions warn about exactly this at runtime. Mapping to `Page<WikimediaEventResponse>` fixes the element type, which is the important half. Returning your own envelope so that pagination metadata is a contract you own is better still.

This lesson uses `Page<WikimediaEventResponse>`, because the element mapping is the lesson and the envelope is a follow-on exercise.

### Aggregations without `Object[]`

The obvious way to count events per wiki:

```java
@Query("SELECT e.wiki, COUNT(e) FROM WikimediaEvent e GROUP BY e.wiki")
List<Object[]> countGroupedByWiki();
```

and then, in the service, `(String) row[0]` and `(Long) row[1]`.

Two unchecked casts and positional indexing, with nothing checked until runtime. Swap the columns in the query and it compiles, deploys, and throws `ClassCastException` in production.

Spring Data supports interface projections, which give the compiler something to work with:

```java
public interface WikiCount {
    String getWiki();
    long getCount();
}
```

The `AS` aliases in the query must match the getter names, so the mapping is by name rather than by position. Rename a column in the query without renaming the getter and you get a failure at startup rather than a wrong cast under load.

---

## Hands-on

### 1. Build the wrong version first

This takes two minutes and it is worth doing, because the output is more persuasive than the argument.

```java
package com.example.wikimedia.consumer.controller;

import com.example.wikimedia.consumer.entity.WikimediaEvent;
import com.example.wikimedia.consumer.repository.WikimediaEventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/wikimedia")
public class WikimediaEventController {

    private final WikimediaEventRepository repository;

    public WikimediaEventController(WikimediaEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/events")
    public Page<WikimediaEvent> events(Pageable pageable) {
        return repository.findAll(pageable);
    }
}
```

Run it and look at what you just published:

```bash
curl -s 'localhost:8082/api/v1/wikimedia/events?size=1' | head -40
```

Three things are now part of your public API. Your database column names. Your Kafka partition and offset for every event. And Spring Data's entire pagination envelope, including `pageable`, `sort`, `first`, `last` and `empty`.

You have also skipped the service layer entirely, so there is nowhere for business logic to live when it arrives.

Delete that file. The rest of the lesson builds the version you would keep.

### 2. Repository queries with projections

```java
package com.example.wikimedia.consumer.repository;

import com.example.wikimedia.consumer.entity.WikimediaEvent;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface WikimediaEventRepository
        extends ListCrudRepository<WikimediaEvent, Long>,
                PagingAndSortingRepository<WikimediaEvent, Long> {

    Page<WikimediaEvent> findByWikiOrderByProcessedAtDesc(String wiki, Pageable pageable);

    Page<WikimediaEvent> findByTypeOrderByProcessedAtDesc(String type, Pageable pageable);

    List<WikimediaEvent> findTop10ByOrderByProcessedAtDesc();

    long countByBotTrue();

    /** Aliases must match the getter names on WikiCount. */
    @Query("""
            SELECT e.wiki AS wiki, COUNT(e) AS count
            FROM WikimediaEvent e
            GROUP BY e.wiki
            ORDER BY COUNT(e) DESC
            """)
    List<WikiCount> countGroupedByWiki();

    @Query("""
            SELECT e.type AS type, COUNT(e) AS count
            FROM WikimediaEvent e
            GROUP BY e.type
            ORDER BY COUNT(e) DESC
            """)
    List<TypeCount> countGroupedByType();

    interface WikiCount {
        String getWiki();
        long getCount();
    }

    interface TypeCount {
        String getType();
        long getCount();
    }
}
```

The derived query names are long and that is deliberate: `findByWikiOrderByProcessedAtDesc` is a specification, and Spring Data fails at startup if it cannot map one to the entity. A misspelling here is a startup error, not a runtime surprise.

### 3. Response DTOs

`dto/WikimediaEventResponse.java`:

```java
package com.example.wikimedia.consumer.dto;

import com.example.wikimedia.consumer.entity.WikimediaEvent;
import java.time.Instant;

public record WikimediaEventResponse(
        String type,
        String title,
        String editor,
        boolean bot,
        String wiki,
        Instant occurredAt
) {

    public static WikimediaEventResponse from(WikimediaEvent event) {
        return new WikimediaEventResponse(
                event.getType(),
                event.getTitle(),
                event.getUsername(),
                event.isBot(),
                event.getWiki(),
                event.getEventTimestamp() == null
                        ? null
                        : Instant.ofEpochSecond(event.getEventTimestamp()));
    }
}
```

`dto/EventStatsResponse.java`:

```java
package com.example.wikimedia.consumer.dto;

import java.util.Map;

public record EventStatsResponse(
        long total,
        long byBots,
        long byHumans,
        Map<String, Long> byWiki,
        Map<String, Long> byType
) {}
```

Two things the entity has and these do not: the database identity, and the Kafka provenance. Those exist so you can debug ingestion, which is not something an API client does.

### 4. The service layer

`service/WikimediaEventService.java`:

```java
package com.example.wikimedia.consumer.service;

import com.example.wikimedia.consumer.dto.EventStatsResponse;
import com.example.wikimedia.consumer.dto.WikimediaEventResponse;
import com.example.wikimedia.consumer.repository.WikimediaEventRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    public Page<WikimediaEventResponse> findByWiki(String wiki, Pageable pageable) {
        return repository.findByWikiOrderByProcessedAtDesc(wiki, pageable)
                .map(WikimediaEventResponse::from);
    }

    public Page<WikimediaEventResponse> findByType(String type, Pageable pageable) {
        return repository.findByTypeOrderByProcessedAtDesc(type, pageable)
                .map(WikimediaEventResponse::from);
    }

    public List<WikimediaEventResponse> findRecent() {
        return repository.findTop10ByOrderByProcessedAtDesc().stream()
                .map(WikimediaEventResponse::from)
                .toList();
    }

    public EventStatsResponse stats() {
        long total = repository.count();
        long bots = repository.countByBotTrue();

        return new EventStatsResponse(
                total,
                bots,
                total - bots,
                toMap(repository.countGroupedByWiki(),
                        WikimediaEventRepository.WikiCount::getWiki,
                        WikimediaEventRepository.WikiCount::getCount),
                toMap(repository.countGroupedByType(),
                        WikimediaEventRepository.TypeCount::getType,
                        WikimediaEventRepository.TypeCount::getCount));
    }

    private <T> Map<String, Long> toMap(List<T> rows,
                                        Function<T, String> keyFn,
                                        Function<T, Long> valueFn) {
        // LinkedHashMap so the ORDER BY in the query survives into the JSON.
        Map<String, Long> result = new LinkedHashMap<>();
        for (T row : rows) {
            result.put(keyFn.apply(row), valueFn.apply(row));
        }
        return result;
    }
}
```

`@Transactional(readOnly = true)` on the class does two things worth knowing. It keeps the persistence context open for the duration of a method, so mapping happens inside a transaction rather than during serialization, and it tells Hibernate to skip dirty checking, which is real work avoided on a read path.

The `LinkedHashMap` matters more than it looks. A `HashMap` would discard the `ORDER BY` from the query, so the busiest wiki would appear in an arbitrary position and the ordering you wrote in JPQL would have no effect on the response.

### 5. The controller

```java
package com.example.wikimedia.consumer.controller;

import com.example.wikimedia.consumer.dto.EventStatsResponse;
import com.example.wikimedia.consumer.dto.WikimediaEventResponse;
import com.example.wikimedia.consumer.service.WikimediaEventService;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/wikimedia")
public class WikimediaEventController {

    private final WikimediaEventService service;

    public WikimediaEventController(WikimediaEventService service) {
        this.service = service;
    }

    @GetMapping("/events")
    public ResponseEntity<Page<WikimediaEventResponse>> events(
            @PageableDefault(size = 20, sort = "processedAt") Pageable pageable) {
        return ResponseEntity.ok(service.findAll(pageable));
    }

    @GetMapping("/events/recent")
    public ResponseEntity<List<WikimediaEventResponse>> recent() {
        return ResponseEntity.ok(service.findRecent());
    }

    @GetMapping("/events/wiki/{wiki}")
    public ResponseEntity<Page<WikimediaEventResponse>> byWiki(
            @PathVariable String wiki,
            @PageableDefault(size = 20) Pageable pageable) {
        return ResponseEntity.ok(service.findByWiki(wiki, pageable));
    }

    @GetMapping("/events/type/{type}")
    public ResponseEntity<Page<WikimediaEventResponse>> byType(
            @PathVariable String type,
            @PageableDefault(size = 20) Pageable pageable) {
        return ResponseEntity.ok(service.findByType(type, pageable));
    }

    @GetMapping("/events/stats")
    public ResponseEntity<EventStatsResponse> stats() {
        return ResponseEntity.ok(service.stats());
    }
}
```

`@PageableDefault` sets a bounded page size. Without it a client can request `size=1000000` and your API will attempt it, which is a denial of service you wrote yourself.

No try or catch anywhere. The controller handles HTTP and nothing else.

### 6. Run it and compare

```bash
curl -s 'localhost:8082/api/v1/wikimedia/events?size=2' | head -30
curl -s 'localhost:8082/api/v1/wikimedia/events/recent' | head -20
curl -s 'localhost:8082/api/v1/wikimedia/events/wiki/enwiki?size=2'
curl -s 'localhost:8082/api/v1/wikimedia/events/type/edit?size=2'
curl -s 'localhost:8082/api/v1/wikimedia/events/stats'
```

Compare the `events` output with what step 1 produced. The Kafka provenance is gone, `editor` replaces the column name, and the timestamp is an ISO-8601 instant rather than an epoch integer.

The envelope is still Spring Data's, which is the remaining leak and the first exercise.

---

## Try it yourself

1. Replace `Page<WikimediaEventResponse>` with your own envelope record carrying `content`, `page`, `size` and `totalElements`. Compare the JSON. Which fields did Spring Data expose that you now do not, and which of them would you have been happy for a client to depend on?

2. Rewrite `countGroupedByWiki` to return `List<Object[]>` and map it with positional casts. Then swap the two columns in the query. Confirm it compiles, deploys, and fails at runtime. This is the failure mode projections exist to prevent.

3. Request `size=100000`. What happens with `@PageableDefault` in place, and what happens without it? Find the property that caps the maximum page size globally.

4. Add a lazy association to the entity, then return the entity directly from a controller method. Trigger the serialization and read the exception. Explain why it occurred during serialization rather than during the query.

---

## Common mistakes

**Returning the entity.**
Your column names, your ingestion internals and your storage schema become your public API.

**Returning `Page<Entity>`.**
Both problems at once: the leaked element type and Spring Data's internal envelope.

**Making the response DTO a field-for-field copy.**
You paid the mapping cost and got none of the decoupling.

**Injecting the repository into the controller.**
There is then nowhere for business logic to live, and the first requirement that is not a query forces a rewrite.

**Using `Object[]` for aggregations.**
Positional access and unchecked casts, with failures deferred to runtime.

**Returning a `HashMap` for ordered aggregates.**
The `ORDER BY` in your query is silently discarded.

**Omitting a page-size cap.**
A client can ask for a million rows and your service will try.

**Reusing one record for requests and responses.**
They validate and evolve differently, and the shared record ends up wrong for both.

---

## Check your understanding

**1. Returning `Page<WikimediaEvent>` publishes two separate leaks. Name both.**

<details>
<summary>Reveal answer</summary>

The element type and the envelope.

`WikimediaEvent` is the entity, so its field names, its database identity and its Kafka provenance all appear in the JSON. Any schema change becomes a breaking API change.

`Page` is Spring Data's internal pagination model, so `pageable`, `sort`, `first`, `last`, `numberOfElements` and `empty` become part of your contract too. That shape has changed between Spring Data versions, which means a dependency upgrade can break your clients without a single line of your own code changing.

Mapping the element type fixes the first. Only your own envelope fixes the second.

</details>

**2. Why does mapping to a DTO inside a `@Transactional(readOnly = true)` service method matter?**

<details>
<summary>Reveal answer</summary>

Because it decides whether lazy loading happens somewhere you control.

Inside the transaction, the persistence context is open, so touching an association loads it and any resulting query is inside the transaction boundary. If you instead returned entities and let Jackson map them during serialization, the context would already be closed, and the lazy getter would throw `LazyInitializationException` or, worse, silently open a new connection per element.

`readOnly = true` adds a second benefit: Hibernate skips dirty checking on the loaded entities, which on a page of results is measurable work avoided.

</details>

**3. Interface projections require `AS` aliases matching the getter names. What does that buy over `Object[]`?**

<details>
<summary>Reveal answer</summary>

Failure at startup instead of failure in production.

With `Object[]` the mapping is positional and untyped, so `(String) row[0]` compiles regardless of what the query actually selects. Swap the columns and the code still compiles and deploys, then throws `ClassCastException` on the first request.

With a projection the mapping is by name and the return type is checked. If the alias and the getter disagree, Spring Data cannot satisfy the interface and tells you when the context starts, before any traffic arrives.

</details>

**4. Your response DTO renames `username` to `editor` and drops `kafkaOffset`. Is that gratuitous churn?**

<details>
<summary>Reveal answer</summary>

No, both changes are the DTO doing its job.

`username` is named for the column, and the column is named `editor` because `user` is a reserved SQL keyword, as Lesson 18 explained. That is a storage constraint leaking into a Java field name, and there is no reason for an API client to inherit it.

`kafkaOffset` exists so you can find the record that produced a row. It is an ingestion detail, and publishing it invites clients to depend on it, at which point changing your ingestion becomes a breaking API change.

If the DTO had been a mechanical copy, both of those would have become permanent parts of your contract by accident rather than by decision.

</details>

**5. Step 1 injected the repository straight into the controller and worked fine. What breaks first as the API grows?**

<details>
<summary>Reveal answer</summary>

The first requirement that is not a query.

As long as every endpoint is a thin passthrough, the service layer looks like ceremony. Then something arrives that is genuinely logic: statistics combining several queries, a rule about which events are visible, a computed field, a cache. It has to live somewhere, and the only two options left are the controller, which then knows about business rules and HTTP, or the repository, which then knows about business rules and persistence.

The `stats()` method in this lesson is exactly that case. It performs four queries and combines them, and it needs ordering preserved into a map. In a controller that would be HTTP handling mixed with aggregation logic, untestable without a web context.

Adding the layer up front costs one file. Adding it later costs a rewrite of every endpoint that grew around its absence.

</details>

---

## Recap

Entities stop at the service layer, because returning them publishes your column names, your ingestion internals and your storage schema as an API contract. You built that version first and looked at the JSON, then replaced it with response DTOs that deliberately diverge from the entity.

`Page<T>` leaks Spring Data's pagination model, and mapping the element type fixes only half of it. Interface projections replace `Object[]` and move aggregation mistakes from production to startup.

The controller handles HTTP, the service owns logic and transactions, and the repository is an interface.

**Next:** [Lesson 24: Testing Kafka with Testcontainers](24-testing-with-testcontainers.md)
