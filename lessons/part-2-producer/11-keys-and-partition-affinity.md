# Lesson 11: Keys and Partition Affinity in Code

> **Part 2: The Producer**

---

## What you'll learn

- How to send a keyed record with `KafkaTemplate`
- How to choose a key, and how to recognise a bad one
- How to prove partition affinity from your own application
- When not to use a key at all

---

## Why this matters

Lesson 04 showed you the mechanism from the CLI: same key, same partition, therefore ordered. This lesson is where that decision moves inside your application, and it is a decision rather than a detail.

The producer you have written so far sends no key:

```java
kafkaTemplate.send(TOPIC, message);
```

Every record is placed by the built-in partitioner. For counting independent events that is fine and it is fast. But it means there is no ordering guarantee between two records about the same subject, and the moment someone asks for "the history of X, in order", the producer has to change.

You are going to make that change and keep it for the rest of the course.

---

## Before you start

[Lesson 10](10-acks-and-idempotence.md), with `acks: all` and idempotence configured.

---

## The concept

### `send()` has an overload

```java
kafkaTemplate.send(String topic, V value);            // no key
kafkaTemplate.send(String topic, K key, V value);     // keyed
```

That is the entire API difference. The second form sets the record's key, the client hashes it, and the partition follows from `murmur2(key) % partitionCount`.

### The key is the ordering decision

A key does exactly one thing for you: it guarantees that records sharing it land on the same partition, and therefore that they are read in the order you sent them.

Everything else follows from that. Kafka does not index by key, does not deduplicate by key, and does not let you fetch by key. On a normal topic the key is a routing input and nothing more.

There is one exception worth knowing about now, because it explains why keys look more powerful than they are. On a **compacted** topic Kafka retains only the most recent record per key, which makes the topic behave like a key-value snapshot. That is how `__consumer_offsets` works, as you saw in Lesson 02. Your topics use time-based retention, so no such thing happens.

### Choosing a key

The question to ask is: which records must never be reordered relative to each other? The answer is your key.

For a stream of page edits the natural answer is the page. Two edits to the same page must be applied in order; two edits to different pages need no relationship at all.

That gives the shape of a good key:

| Property | Why |
|---|---|
| Enough distinct values to spread across partitions | otherwise you create a hot partition |
| No single value dominating traffic | one hot key is one hot partition |
| Stable over time | a key that changes breaks the affinity it existed to provide |
| Relevant to the ordering requirement | keying by something irrelevant costs throughput and buys nothing |

The failure mode at both extremes is instructive. Key by `country` and one country's traffic swamps a partition. Key by a fresh UUID per record and you get perfect distribution with zero ordering benefit, having paid for hashing and given up the larger batches you would have had with no key at all.

### What a key costs

Two things, and Lesson 04's last quiz question covered the second.

**You lose adaptive partitioning.** With no key the producer can route around a slow broker, because it is free to choose. With a key the partition is determined arithmetically, so if that partition's leader is slow, your producer waits.

**You lose the ability to repartition freely.** Adding partitions changes the modulus, so a key's history splits across old and new partitions and per-key ordering breaks across the boundary.

---

## Hands-on

### 1. Add the key parameter

`src/main/java/com/example/wikimedia/producer/kafka/WikimediaProducer.java`:

```java
package com.example.wikimedia.producer.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WikimediaProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class);
    private static final String TOPIC = "wikimedia-stream";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String message) {
        kafkaTemplate.send(TOPIC, key, message);
        log.info("Sent key={}", key);
    }
}
```

One extra argument. That is the whole change to the send path, and this signature stays for the rest of the course: the real producer in Lesson 14 keys by the page being edited, and Part 3's consumer relies on it.

### 2. Send records with repeating keys

Update the throwaway `StartupMessageSender`:

```java
package com.example.wikimedia.producer;

import com.example.wikimedia.producer.kafka.WikimediaProducer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class StartupMessageSender implements ApplicationRunner {

    private final WikimediaProducer producer;

    public StartupMessageSender(WikimediaProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(ApplicationArguments args) {
        producer.sendMessage("en.wikipedia", "edit-1");
        producer.sendMessage("de.wikipedia", "edit-2");
        producer.sendMessage("en.wikipedia", "edit-3");
        producer.sendMessage("fr.wikipedia", "edit-4");
        producer.sendMessage("en.wikipedia", "edit-5");
        producer.sendMessage("de.wikipedia", "edit-6");
    }
}
```

### 3. Prove the affinity

Delete and recreate the topic first, so offsets start clean. Your application recreates it on startup from the `NewTopic` bean in Lesson 09:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
  --delete --topic wikimedia-stream
```

```bash
./mvnw spring-boot:run
```

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream \
  --from-beginning --max-messages 6 \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property key.separator=' | '
```

```
Partition:2 | fr.wikipedia | edit-4
Partition:0 | de.wikipedia | edit-2
Partition:0 | de.wikipedia | edit-6
Partition:1 | en.wikipedia | edit-1
Partition:1 | en.wikipedia | edit-3
Partition:1 | en.wikipedia | edit-5
```

The same partition numbers the CLI produced in Lesson 04, because it is the same algorithm: the Java client and the console producer both hash with `murmur2`. Given three partitions, `en.wikipedia` lands on partition 1 everywhere.

Within partition 1: `edit-1`, `edit-3`, `edit-5`, in the order you sent them. That is the guarantee.

Note again that the partitions came back in an arbitrary order. Ordering holds inside a partition and nowhere else.

### 4. See the hot partition

Now make skew obvious. Send twenty records, seventeen of them for one key:

```java
    @Override
    public void run(ApplicationArguments args) {
        for (int i = 0; i < 17; i++) {
            producer.sendMessage("en.wikipedia", "edit-" + i);
        }
        producer.sendMessage("de.wikipedia", "edit-a");
        producer.sendMessage("fr.wikipedia", "edit-b");
        producer.sendMessage("es.wikipedia", "edit-c");
    }
```

Recreate the topic, run, then count:

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server kafka-1:29092 --topic wikimedia-stream
```

One partition holds seventeen records and the others hold one or two. `murmur2` distributed the four *keys* reasonably, and could do nothing about one key carrying 85% of the traffic.

This is a hot partition, and it is the most common Kafka scaling failure. The consumer assigned to that partition does most of the work while its peers idle, and adding consumers cannot help, because the partition count caps parallelism and one partition goes to one member.

No producer setting fixes this. The fix is a different key, or a compound key such as page plus a bucket number, accepting ordering only within a bucket.

### 5. Put the sender back

Restore the six-record version from step 2. Lesson 14 replaces this class with real data.

---

## Try it yourself

1. Send the key `en.wikipedia` to a topic with 4 partitions rather than 3. It moves to partition 0, as you saw from the CLI in Lesson 04. Now explain what would have to happen to a year of existing records for that move to be safe.

2. Send 1,000 records with keys `user-0` through `user-999`, then check the per-partition offsets. Is the split even? Compare it with the keyless 50,000-record run from Lesson 04 and say which mechanism produced the more even distribution, and why that does not make it the better choice.

3. Send a record with an explicitly null key, using `kafkaTemplate.send(TOPIC, null, message)`. Which partition does it land on, and which code path did it take? What does that tell you about the difference between "no key" and "a key that happens to be null"?

4. You need ordering per page, but one page produces 90% of the traffic. Design a keying scheme that keeps ordering where it matters and spreads the load, then state precisely which ordering guarantee you gave up to get it.

---

## Common mistakes

**Assuming a key gives you lookup by key.**
It gives you routing, and therefore ordering. Kafka has no index on keys, and on a time-retained topic the key is only a partitioning input.

**Keying by something with low cardinality.**
Country, region, event type, or tenant when one tenant dominates. Each puts most of your traffic on one partition.

**Keying by a unique value per record.**
Perfect distribution, no ordering benefit, and larger batches given up for nothing. If nothing needs ordering, send no key.

**Assuming an even key distribution means an even load distribution.**
The hash spreads keys, not traffic. One busy key is one busy partition.

**Adding partitions to fix a hot partition.**
It re-maps every key, breaks per-key ordering across the change, and the hot key is still one key on one partition afterwards.

---

## Check your understanding

**1. Records with the same key always land on the same partition. What exactly does that buy you?**

<details>
<summary>Reveal answer</summary>

One thing: relative ordering. Records sharing a key occupy one partition, one partition is read in log order by exactly one member of a consumer group, so those records are processed in the order you sent them.

It does not buy lookup, deduplication or grouping on the consumer side. A consumer reading that partition also receives every other key that hashed there, interleaved, and has to sort out which records belong to which key itself.

</details>

**2. Your topic is keyed by page and one page produces most of the traffic. You add consumers to catch up. What happens?**

<details>
<summary>Reveal answer</summary>

Nothing improves. The busy key hashes to one partition, that partition is assigned to one member of the group, and the additional consumers get other partitions or nothing at all.

Lag on that one partition keeps climbing while the rest of the group idles. Aggregate lag looks moderate, which is what makes this hard to spot on a dashboard that sums lag across partitions instead of showing the worst one.

</details>

**3. Why does a key remove the producer's ability to route around a slow broker?**

<details>
<summary>Reveal answer</summary>

Because with a key there is no routing decision left to make.

The modulus names a partition, that partition has exactly one leader, and the record must go to that leader. Adaptive partitioning, which is on by default, can only help when the producer is free to choose, and that is the keyless case.

So a hot key whose partition sits on a slow broker is doubly bad, and no producer setting will rescue it.

</details>

**4. You switch a topic from unkeyed to keyed and throughput drops. Why?**

<details>
<summary>Reveal answer</summary>

Because batching is per partition, and keying scatters your records across all partitions immediately instead of filling one at a time.

Unkeyed, the producer accumulated roughly `batch.size` bytes for a single partition before switching, producing large well-compressed requests. Keyed, records are spread by hash, so each partition's batch fills more slowly and is sent smaller, or later, or both.

That is the real trade: ordering per key costs batch efficiency. It is usually right when ordering matters and pure loss when it does not, which is why the choice deserves a deliberate answer rather than a default.

</details>

**5. Lesson 04 warned that increasing partitions on a keyed topic is a breaking change. Given the producer you just wrote, what would you actually have to do to repartition safely?**

<details>
<summary>Reveal answer</summary>

Create a new topic with the target partition count, re-key the existing data into it, and move consumers across.

Increasing the count in place leaves old records where the old modulus put them and sends new records where the new modulus points. A key's history is then split across two partitions with no ordering relationship between them, and nothing in Kafka records that this happened.

Copying into a fresh topic works because every record is re-hashed under the new count, so each key ends up wholly on one partition again. The cost is a migration: two topics live at once, consumers cut over at a chosen offset, and any group that had committed positions on the old topic needs a defined starting point on the new one.

This is why partition counts on keyed topics are chosen with room to grow, and why Lesson 09 treated `.partitions()` as the one declaration you cannot casually edit.

</details>

---

## Recap

A key is a routing input that buys exactly one guarantee: records sharing it are ordered relative to each other. You changed `sendMessage` to take a key, proved the affinity from your own application, and produced a hot partition on purpose to see what a bad key does.

Choosing a key means answering which records must never be reordered. Anything more specific wastes throughput, and anything less specific creates a hot partition.

**Next:** [Lesson 12: Batching, Linger and Compression](12-batching-linger-compression.md)
