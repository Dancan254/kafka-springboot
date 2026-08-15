# Lesson 15 — Your First `@KafkaListener`

> **Part 3 — The Consumer** · 25 minutes

---

## What you'll learn

- How `@KafkaListener` turns a method into a consumer group member
- What `groupId` and `auto-offset-reset` decide, and when each one applies
- What the poll loop is, and why it exists even though you never call `poll()`
- How to read a `ConsumerRecord` and see the partition and offset your record came from

---

## Why this matters

Everything in Part 1 you did by hand: joined a group, read records, watched offsets move. `@KafkaListener` does all of it for you in one annotation, and that's exactly the danger. Three lines of code hide a background thread, a group membership, a rebalance protocol, and an offset commit strategy.

You already understand every one of those. This lesson connects the annotation to the machinery you've seen.

---

## Before you start

[Lesson 14](14-wikimedia-sse-stream.md), and a producer that can fill the topic. Cluster healthy.

You'll build the consumer as a **second Spring Boot application**, on port 8082. Producer and consumer are separate services, as they'd be in production.

---

## The concept

### The poll loop you never write

The raw Kafka consumer API is a loop:

```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
    for (ConsumerRecord<String, String> record : records) {
        process(record);
    }
    consumer.commitSync();
}
```

Spring's **listener container** runs this loop on a background thread and calls your annotated method once per record. `@KafkaListener` is the annotation that registers your method with a container.

This matters because the loop has properties you inherit whether you know it or not:

- **A single thread owns a consumer**, and it must return to `poll()` regularly. If your method takes longer than `max.poll.interval.ms` (5 minutes by default), the broker assumes you're dead and rebalances your partitions away.
- **Records are delivered in batches**, then handed to you one at a time. `max-poll-records` (500) caps how many arrive per `poll()`.
- **Polling is what sends heartbeats** — well, it used to be. Modern clients heartbeat on a separate thread, which is why `session.timeout.ms` and `max.poll.interval.ms` are two different settings.

### `groupId` — the most consequential string in your app

From Lesson 05: a group owns committed offsets, and within a group each partition goes to exactly one member.

`@KafkaListener(groupId = "wikimedia-consumer-group")` joins that group. Two consequences that catch people out:

**Two applications sharing a `groupId` split the partitions.** Each sees only *some* records. If `billing` and `analytics` both use `"my-group"`, each processes roughly half the events and both look like they're working. This is a nasty bug because there's no error — just silent, partial data.

**Changing the `groupId` resets your position.** A new group has no committed offsets, so `auto-offset-reset` decides where it starts. Renaming a group in a config file can replay your entire topic, or skip everything in it.

### `auto-offset-reset` — only for a group with no offset

```yaml
auto-offset-reset: earliest
```

This applies in exactly one situation: **the group has no valid committed offset for a partition.** That happens when the group is brand new, or when its committed offset points at a record that retention has already deleted.

| Value | Behaviour |
|---|---|
| `earliest` | start at the oldest retained record |
| `latest` | start at the end — only records produced from now on |
| `none` | throw an exception |

The default is `latest`. That's why `kafka-console-consumer` printed nothing in Lesson 03 until you passed `--from-beginning`.

Once the group *has* committed an offset, this setting is ignored entirely. It is not "always start from the beginning" — it is "where do I start when I have no idea where I was."

Choose `earliest` when reprocessing history is safe and desirable. Choose `latest` for a live dashboard where old events are worthless. Choose `none` when a missing offset means something has gone badly wrong and you'd rather crash than guess.

### Deserializers mirror serializers

The producer serialized `String → byte[]`. The consumer must do the reverse:

```yaml
key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

These must correspond to what the producer wrote. Deserialize Avro bytes with a `StringDeserializer` and you get mojibake, not an exception — which is one of several reasons Lesson 25 exists.

---

## Hands-on

### 1. Create the consumer project

A second app: group `com.javaguy`, artifact `consumer`, Java 25, Spring Boot 4.1.0.

`pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.0</version>
        <relativePath/>
    </parent>

    <groupId>com.javaguy</groupId>
    <artifactId>consumer</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <properties>
        <java.version>25</java.version>
    </properties>

    <dependencies>
        <!-- The starter, not org.springframework.kafka:spring-kafka.
             See Lesson 08 — the bare library gives you no auto-configuration. -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 2. `src/main/resources/application.yml`

```yaml
spring:
  application:
    name: consumer

  kafka:
    # Declared at the top level, not under consumer:, because the DLT producer
    # in Lesson 21 will need it too.
    bootstrap-servers: localhost:9092,localhost:9093,localhost:9094

    consumer:
      group-id: wikimedia-consumer-group

      # Only consulted when this group has no committed offset for a partition.
      # Once it has one, this setting is ignored.
      auto-offset-reset: earliest

      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

      # How many records one poll() may return. Not a batch size for your method —
      # Spring still hands them to you one at a time.
      max-poll-records: 500

server:
  port: 8082
```

Note `server.port: 8082`. The producer owns 8081.

### 3. `ConsumerApplication.java`

```java
package com.javaguy.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
```

### 4. `consumer/WikimediaConsumer.java`

```java
package com.javaguy.consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WikimediaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaConsumer.class);

    @KafkaListener(
            topics = "wikimedia-stream",
            groupId = "wikimedia-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Consumed | partition={} offset={} valueLength={}",
                record.partition(), record.offset(), record.value().length());
    }
}
```

Take `ConsumerRecord<String, String>` rather than a bare `String`. The bare version gives you the value and hides everything else. `ConsumerRecord` gives you `partition()`, `offset()`, `key()`, `timestamp()`, and `headers()` — and in Lesson 22 the headers are the whole point.

> No `@EnableKafka` needed. Spring Boot's auto-configuration adds it when the starter is present.

### 5. Run it

Start the **consumer** first, then the producer. That ordering matters less than you'd think — `auto-offset-reset: earliest` means a brand-new group will read the whole topic anyway — but it's the habit to build.

```bash
./mvnw spring-boot:run
```

If the topic already holds records from Part 2, they replay immediately:

```
Consumed | partition=0 offset=0 valueLength=1834
Consumed | partition=0 offset=1 valueLength=1502
Consumed | partition=2 offset=0 valueLength=2011
...
```

**Partitions interleave.** You'll see offsets from partition 0, then 2, then 1, then 0 again. Each partition is ordered internally; across partitions there is no order at all. Exactly as promised in Lesson 01, now visible in your own logs.

Now start the producer in another terminal, and trigger the stream:

```bash
curl http://localhost:8081/api/v1/wikimedia
```

Live Wikipedia edits scroll past.

### 6. Prove `auto-offset-reset` only fires once

Stop the consumer. Restart it.

It does **not** replay from the beginning. It resumes from its committed offsets, because the group now has them. `auto-offset-reset: earliest` was consulted on the first run and ignored on the second.

Now change `groupId` to `"wikimedia-consumer-group-v2"` and restart. Everything replays from offset 0 — a brand-new group with no offsets, so `earliest` applies again.

Check both groups exist:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 --list
```

Two groups, two independent positions over the same data. That's Lesson 05's independence, from Java.

Change the `groupId` back before continuing.

### 7. Watch the group from the outside

While the consumer runs:

```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group wikimedia-consumer-group
```

```
GROUP                    TOPIC            PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID
wikimedia-consumer-group wikimedia-stream 0         1322           1322           0   consumer-...-1
wikimedia-consumer-group wikimedia-stream 1         1098           1098           0   consumer-...-2
wikimedia-consumer-group wikimedia-stream 2         772            772            0   consumer-...-3
```

`LAG 0` across all three, and a live `CONSUMER-ID` per partition. Your `@KafkaListener` is a fully paid-up member of the group you inspected by hand in Lesson 05.

---

## Try it yourself

1. Add a second `@KafkaListener` method, in a different class, with the **same** `groupId` and the same topic. Run one instance of the app. Do both methods receive every record? Now give the second one a different `groupId`. What changed, and why?

2. Add `Thread.sleep(400)` inside `consume()`. Watch the lag column climb. How long until the group is evicted, and which setting governs that — `session.timeout.ms` or `max.poll.interval.ms`?

3. Set `auto-offset-reset: none` and use a brand-new `groupId`. What exception do you get, and why might that be the setting you actually want in a financial system?

---

## Common mistakes

**Sharing a `groupId` between two different applications.**
They split the partitions. Each app sees a subset of records and neither errors. Two apps needing all records need two group IDs.

**Believing `auto-offset-reset: earliest` always replays.**
It only applies when the group has no committed offset. After the first run it is dead configuration.

**Changing `groupId` casually.**
It's a position reset. With `earliest`, you reprocess everything; with `latest`, you skip everything currently pending.

**Doing slow work inside the listener.**
The thread must return to `poll()` within `max.poll.interval.ms`. Exceed it and the broker rebalances your partitions to someone else — while you're still working on them.

**Taking `String` instead of `ConsumerRecord`.**
You lose partition, offset, key, timestamp, and headers. The value is rarely all you need.

**Assuming records arrive in global order.**
They arrive ordered per partition, interleaved across partitions.

---

## Check your understanding

**1. Your consumer has `auto-offset-reset: earliest`. It runs, processes 10,000 records, and you restart it. How many does it reprocess?**

<details>
<summary>Reveal answer</summary>

None. It resumes exactly where it left off.

`auto-offset-reset` is consulted only when the group has **no valid committed offset** for a partition. After the first run, `__consumer_offsets` holds a position for every partition, so the setting is never read.

The one exception: if the committed offset points to a record that retention has since deleted, the offset is no longer valid, and `earliest` kicks in again — silently replaying from the oldest retained record.

</details>

**2. Two teams deploy services that both read `orders`. Both used the default `groupId` from a copy-pasted config. What do they observe?**

<details>
<summary>Reveal answer</summary>

Each service processes roughly half the orders, and neither logs an error.

Sharing a `groupId` makes them members of the **same consumer group**, so Kafka splits the partitions between them — that's the load-balancing behaviour from Lesson 05, working exactly as designed. With 3 partitions and 2 members, one gets 2 partitions and the other gets 1.

Both services look healthy. Lag is zero. Every order is processed exactly once — by one of the two services, unpredictably. The bug surfaces as "some orders never got an invoice," and it looks like data loss.

Two applications that each need every record must use **different** group IDs.

</details>

**3. You never call `poll()`. So what is actually fetching records, and why does that mean a slow listener method is dangerous?**

<details>
<summary>Reveal answer</summary>

Spring's **listener container** runs the poll loop on a background thread. It calls `poll()`, receives up to `max-poll-records` records, and invokes your method once per record before returning to `poll()`.

Your method therefore executes *inside* the poll loop, on the consumer's thread. If processing 500 records takes longer than `max.poll.interval.ms` (default 5 minutes), the broker concludes the member is dead, evicts it from the group, and rebalances its partitions to another consumer — which starts reprocessing them from the last committed offset.

Meanwhile your "dead" consumer is still working, and when it finishes and tries to commit, the commit fails because it no longer owns those partitions. Slow processing doesn't just add lag; it causes rebalance storms and duplicate work.

</details>

**4. Your consumer logs offsets in the sequence 0, 0, 1, 0, 1, 2 with partitions 0, 2, 0, 1, 2, 0. Is anything wrong?**

<details>
<summary>Reveal answer</summary>

No, that's correct behaviour.

Offsets are per partition — every partition has its own offset 0. The sequence shows partition 0 at offset 0, partition 2 at offset 0, partition 0 at offset 1, and so on. Within each partition the offsets increase monotonically, which is the only ordering Kafka promises.

A single `poll()` returns records from several partitions at once, and the container iterates them partition by partition. Interleaving across partitions is expected and unavoidable — it's the direct cost of the parallelism partitions buy you.

</details>

**5. Why is `auto-offset-reset: none` a defensible choice for a payments system, when it makes the consumer throw on startup?**

<details>
<summary>Reveal answer</summary>

Because both alternatives silently do something wrong, and in payments "wrong" is expensive.

If a committed offset has vanished — expired past retention, or the group was accidentally deleted — then `earliest` **reprocesses history**, potentially double-charging customers. And `latest` **skips every pending record**, silently dropping payments that were never processed.

`none` throws `NoOffsetForPartitionException`. The consumer refuses to start, a human investigates, and someone makes a deliberate decision about where to resume.

It converts silent data corruption into a loud, blocking failure. That's the right trade when the cost of being wrong exceeds the cost of being down — the same reasoning behind `min.insync.replicas` rejecting writes in Lesson 06.

</details>

---

## Recap

`@KafkaListener` registers your method with a listener container that runs the poll loop for you. `groupId` decides which offsets you own and who you share partitions with. `auto-offset-reset` decides where you start *only when you have no committed offset*. And your method runs on the polling thread, so slow processing triggers rebalances rather than merely adding lag.

Right now you're logging the length of a JSON string. Time to read what's inside it.

**Next:** [Lesson 16 — DTO records & deserialization →](16-dtos-and-deserialization.md)
