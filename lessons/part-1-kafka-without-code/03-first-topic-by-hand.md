# Lesson 03: Create a Topic, Produce and Consume by Hand

> **Part 1: Kafka Without Code**

---

## What you'll learn

- How to create a topic from the CLI and read its description
- How to write records with `kafka-console-producer` and read them back
- Why `--from-beginning` exists, and what happens without it
- How to see the partition and offset attached to every record

---

## Why this matters

In Part 2 you will write `kafkaTemplate.send(...)` and a record will appear in a topic. If that is the first time you have ever produced to Kafka, and it does not work, you will have no way to tell whether the bug is in your Spring configuration, your serializer or the broker.

Doing it by hand once, with two shell commands and no framework in the way, gives you a baseline. Afterwards, when the Java version misbehaves, you can drop to the CLI and ask Kafka directly.

---

## Before you start

[Lesson 02](02-tour-of-kafka-ui.md), with your broker healthy:

```bash
docker compose ps kafka-1
```

---

## The concept

Every Kafka CLI tool needs a `--bootstrap-server`: an address to ask "where is everything?". The client connects there, receives the cluster's topology, and then talks to whichever broker it actually needs.

You will run these tools *inside* the `kafka-1` container using `docker exec`, because the container already has them on its `PATH`. That is also why the course uses Confluent's image: `kafka-topics` and friends are ready to run, with no path prefix and no `.sh` suffix.

Inside the container, `--bootstrap-server localhost:9092` works, because the broker is listening on that port in that container. It keeps every command in Part 1 short.

That will stop being true in Lesson 06. Once there are three brokers, `localhost:9092` inside `kafka-1` reaches only `kafka-1`, and the metadata it returns will point at addresses the other two brokers do not answer on. Lesson 06 shows the exact failure and switches to the internal listener. For now, one broker, one address, no complications.

### What a topic actually costs to create

Creating a topic is a metadata operation. The broker appends a record to the metadata log, the same log you read in Lesson 01, and then creates a directory per partition on disk. There is no schema, no table definition and no size to declare up front. A topic is a name, a partition count, a replication factor and a handful of configuration overrides.

That cheapness is why auto-creation exists, and why it is a trap. More on that in Lesson 09.

---

## Hands-on

### 1. Create a topic

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic lessons-demo \
  --partitions 1 \
  --replication-factor 1
```

```
Created topic lessons-demo.
```

One partition, one copy.

One partition means total ordering across the whole topic, which is convenient for a first experiment and rarely what you want in production. Replication factor 1 is not a choice: you have one broker, and a replica is a copy on a *different* broker, so 1 is the maximum available. Ask for more and the broker refuses. Lesson 06 raises both numbers.

### 2. Look at what you made

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic lessons-demo
```

```
Topic: lessons-demo	TopicId: HRuos5JuQL66i-EQ6qYESQ	PartitionCount: 1	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: lessons-demo	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
```

Read that second line carefully. It is the most information-dense output in all of Kafka, and every field on it changes in Lesson 06:

- **`Partition: 0`** is the only partition.
- **`Leader: 1`** means broker 1 handles every read and write for it. With one broker there was no decision to make.
- **`Replicas: 1`** lists every broker holding a copy. The first entry is also the *preferred leader*, the broker Kafka hands leadership back to when it can.
- **`Isr: 1`** lists the replicas that are **in sync** with the leader. This is the number that moves when a broker fails.
- **`Elr`** and **`LastKnownElr`** are Eligible Leader Replicas, a Kafka 4 durability feature that tracks replicas known to have been complete before an outage. Empty here, and empty on any healthy cluster.

`Configs: min.insync.replicas=1` is worth noting now: it is the only topic-level override in play, and Lesson 06 is largely about what happens when you raise it.

### 3. Produce three records

```bash
docker exec -it kafka-1 kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic lessons-demo
```

You get a `>` prompt. Each line you type and enter is one record. Type three:

```
>hello kafka
>second message
>third message
```

Press **Ctrl-D** to exit.

Nothing was printed back. The producer sent three records and the broker accepted them silently. That silence is worth noticing: Kafka told you nothing about which partition or offset they landed on. The information exists, and the console producer discards it. Your Java producer will not have to, which is Lesson 13.

### 4. Read them back

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic lessons-demo \
  --from-beginning \
  --max-messages 3
```

```
hello kafka
second message
third message
Processed a total of 3 messages
```

Now run the same command **without** `--from-beginning`. It hangs, printing nothing.

That is not a bug, and it is the most common "Kafka is broken" moment for beginners.

A brand-new consumer has no committed position, so `auto.offset.reset` decides where it starts, and its default is `latest`: only records that arrive after I connect. Nothing is being produced, so it waits. `--from-beginning` sets that to `earliest` instead.

Leave it hanging. In a second terminal, produce another record:

```bash
echo 'live message' | docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server localhost:9092 --topic lessons-demo
```

`live message` appears in the first terminal immediately. Press Ctrl-C to stop it.

### 5. See partitions and offsets

The consumer can show you the metadata Kafka attached to each record:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic lessons-demo \
  --from-beginning \
  --max-messages 4 \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

```
Partition:0	Offset:0	hello kafka
Partition:0	Offset:1	second message
Partition:0	Offset:2	third message
Partition:0	Offset:3	live message
```

There is Lesson 01's model made concrete: one partition, offsets counting up from 0, in the order you wrote them.

> Older tutorials pass these as `--property print.partition=true`. That still works in Kafka 4.x but prints a deprecation warning, because the flag was split by role: `--formatter-property` for the consumer's output formatter, `--reader-property` for the console producer's input reader, and `--command-property` for the underlying client's own configuration. If you see `Option --property is deprecated`, that is why. The older `--producer-property` is deprecated in the same release, also in favour of `--command-property`.

### 6. Ask how many records exist

```bash
docker exec kafka-1 kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic lessons-demo
```

```
lessons-demo:0:4
```

That is `topic:partition:end-offset`. The end offset is the offset the *next* record will receive, so `4` means offsets 0 to 3 exist. Four records.

### 7. Watch an internal topic get created

In Lesson 02 the topic list was empty. Look again:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

```
__consumer_offsets
lessons-demo
```

You never created `__consumer_offsets`. Your console consumer did, indirectly: it joined a consumer group, committed a position, and the broker created the topic to hold it. Confirm the group exists:

```bash
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

```
console-consumer-73947
```

Your number will differ. `kafka-console-consumer` invents a random group name when you do not give it `--group`, which is exactly why step 4 replayed all the records instead of resuming: each run was a different group with no committed position.

That is Lesson 05's subject, and you have just created the evidence for it.

### 8. The same thing in Kafka UI

Go to **http://localhost:8080**, then **Topics**, then `lessons-demo`, then its messages view. All four records are there with their partition, offset, timestamp and an empty key.

There is also a control for producing a record from the browser. Send one, then re-run `kafka-get-offsets`. The end offset is 5.

The UI, the CLI and your future Java code are all doing the same thing through the same APIs.

---

## Try it yourself

1. Create a topic `ordering-test` with 3 partitions, produce five records to it, then consume with `--formatter-property print.partition=true`. You will probably find all five on a *single* partition, and not necessarily partition 0. Three partitions were available and the producer used one. Write down your explanation before Lesson 04, which is about exactly this.

2. Consume `lessons-demo` from the beginning twice in a row without a `--group` flag. You get all four records both times. Using step 7, explain why the second run does not resume where the first stopped, and then predict what happens if you pass `--group my-group` to both runs.

3. Try to create a topic with a replication factor your cluster cannot satisfy:

   ```bash
   docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
     --create --topic too-durable --partitions 1 --replication-factor 3
   ```

   Read the error. It names the number of brokers it found. Keep this in mind for Lesson 06, where the same command succeeds.

Keep `lessons-demo` and `ordering-test`. Lesson 04 uses them.

---

## Common mistakes

**Expecting `kafka-console-consumer` to show existing records by default.**
It will not. Without `--from-beginning` you see only records produced after the consumer starts.

**Thinking a hanging consumer means something is broken.**
A consumer with nothing to read blocks. That is its job.

**Forgetting `--max-messages` and wondering why the consumer never exits.**
`--timeout-ms 5000` also works, but it exits with a noisy `TimeoutException` stack trace. `--max-messages` is cleaner when you know the count.

**Using `--property` for formatter options.**
Deprecated in Kafka 4. Use `--formatter-property` on the consumer and `--reader-property` on the producer.

**Assuming a random consumer group is harmless.**
Every `kafka-console-consumer` run without `--group` creates a new group that commits offsets into `__consumer_offsets` and lingers there. Harmless locally, untidy on a shared cluster.

---

## Check your understanding

**1. You run `kafka-console-consumer` with no `--from-beginning` on a topic containing 100 records. Nothing prints. Is the data gone?**

<details>
<summary>Reveal answer</summary>

No. All 100 records are still in the topic.

A new consumer has no committed offset, so `auto.offset.reset` decides where it begins, and its default is `latest`, meaning the end of the log from now on. The consumer is sitting at offset 100 waiting for record 101.

Adding `--from-beginning` sets that to `earliest` and all 100 appear. In Lesson 15 you will set the same thing in `application.yml` as `auto-offset-reset: earliest`, for the same reason.

</details>

**2. `--describe` shows `Replicas: 1` and `Isr: 1`. What is the difference between those two lists, and why can they never differ on your current cluster?**

<details>
<summary>Reveal answer</summary>

**Replicas** is the *assigned* list: which brokers are supposed to hold a copy. It is static until you reassign partitions.

**ISR**, the in-sync replicas, is the *current* list: which of those copies have actually caught up with the leader. It shrinks and grows at runtime.

On your cluster they cannot differ, because the only replica is the leader itself, and a leader is in sync with itself by definition. The lists can only diverge once there is a follower that might fall behind, which is Lesson 06.

</details>

**3. You produce three records to `lessons-demo` and read them back in the order you wrote them. You do the same on `ordering-test`, which has 3 partitions, and again they come back in order. Have you now proved that Kafka preserves order on a 3-partition topic?**

<details>
<summary>Reveal answer</summary>

No. You have proved nothing about the 3-partition case, because the records never used more than one partition.

Kafka's guarantee is ordering *within* a partition. On `lessons-demo` that is the whole topic, so the total order is guaranteed and always will be.

On `ordering-test` the guarantee only applied because exercise 1 put all three records on a single partition: with no key, the producer picks one partition and sticks to it for the batch. You observed a per-partition guarantee and read it as a topic-wide one.

Spread those three records across three partitions and the guarantee is gone. Each partition is an independent log, and nothing orders a consumer's reads across them. Lesson 04 is where you make that happen on purpose, with keys.

</details>

**4. `kafka-get-offsets` reports `lessons-demo:0:4`. Later it reports `lessons-demo:0:9`, and a colleague insists they produced only three more records. Can both be true?**

<details>
<summary>Reveal answer</summary>

Only if something else also produced, which on your cluster it may well have: the messages view in Kafka UI has a produce control, and step 8 used it.

The end offset counts every record ever appended to that partition. It never decreases and never skips backwards, even when retention deletes old records, because deletion moves the *start* offset up rather than the end offset down.

So the end offset is a write counter, not a "how many records can I read" counter. For that you need end offset minus start offset.

</details>

**5. The console producer accepted your three records and printed nothing at all: no partition, no offset, no confirmation. Step 5 then showed that Kafka knew the partition and offset of every one of them. Did the producer have to go back and ask for that information, or did it already have it?**

<details>
<summary>Reveal answer</summary>

It already had it, and threw it away.

A `send()` is a round trip with a reply. The broker's acknowledgment is not a bare "received": it is a `RecordMetadata` carrying the topic, partition, offset and timestamp the record was actually assigned. The producer cannot know its offset before the broker appends it, so the only place that number can come from is the reply.

`kafka-console-producer` receives that reply and discards it, which is why the prompt just returns. Nothing was hidden from it and nothing extra would have to be requested.

Keep hold of that, because two later lessons are consequences of it. Lesson 10 is about `acks`, which is precisely the setting that controls how much of a reply you wait for, up to `acks=0` where you genuinely do not get one. Lesson 13 is about catching the reply you were already being sent.

</details>

**6. Step 7 showed `__consumer_offsets` appearing without you creating it. Lesson 02's second exercise showed the broker config that shaped it. What is the risk of relying on that behaviour in production?**

<details>
<summary>Reveal answer</summary>

The topic is created with whatever the broker defaults happen to be at that moment, and you find out what they were afterwards.

For `__consumer_offsets` specifically, the default replication factor is 3. Start a single-broker cluster without overriding it, as Lesson 00 deliberately did override it, and the first commit fails because the topic cannot be created. The error surfaces in a consumer, far from its actual cause in broker configuration.

The same reasoning applies to your own topics, which is why Lesson 09 declares them explicitly instead of letting them be auto-created.

</details>

---

## Recap

You created a topic, wrote records into it and read them back twice, because reads do not consume. You saw the partition and offset attached to every record, learned that `--from-beginning` exists because a fresh consumer defaults to `latest`, and watched `__consumer_offsets` come into existence the moment a consumer group needed it.

So far every record went to partition 0, because there was only one. Time to change that.

**Next:** [Lesson 04: Partitions and Keys](04-partitions-and-keys.md)
