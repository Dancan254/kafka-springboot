# Lesson 08: Your First KafkaTemplate.send()

> **Part 2: The Producer**

---

## What you'll learn

- How to wire a Spring Boot 4 application to Kafka with the right dependency
- What `KafkaTemplate` is, and where its bean comes from
- Why a serializer is mandatory, and what happens when you omit one
- How to verify from the CLI that your Java code really wrote to a partition

---

## Why this matters

This is the first Java in the course, and it is deliberately the smallest possible thing: an application that starts, writes three strings to a topic, and exits.

Everything you built by hand in Part 1 is about to be produced to by code. When it works, you will confirm it with `kafka-console-consumer`, the tool you already trust. That habit, checking the broker rather than your own log output, is the one worth forming now, because Lesson 13 is going to show you exactly how little your log output proves.

---

## Before you start

[Lesson 07](../part-1-kafka-without-code/07-kraft-no-zookeeper.md), with a healthy three-broker cluster:

```bash
docker compose ps kafka-1 kafka-2 kafka-3
```

You also need **Java 25** and **Maven 3.9 or newer**:

```bash
java -version
mvn -version
```

---

## The concept

### The dependency that actually matters

Spring Boot 4 reorganised auto-configuration. In Boot 3 every auto-configuration class lived in one large `spring-boot-autoconfigure` jar, so putting `spring-kafka` on the classpath was enough: `KafkaAutoConfiguration` was already there, waiting for the `KafkaTemplate` class to appear.

Boot 4 split those into per-technology modules. Kafka's auto-configuration now lives in `spring-boot-kafka`, which you get through the starter:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
```

Depend on `org.springframework.kafka:spring-kafka` directly instead, as a great many tutorials still do, and you get the Kafka library but not the auto-configuration. Your application compiles perfectly and then dies at startup:

```
Parameter 0 of constructor in ...WikimediaProducer required a bean of type
'org.springframework.kafka.core.KafkaTemplate' that could not be found.
```

That error is confusing because the class obviously *is* on the classpath; you can import it. The bean is not, because nothing auto-configured one.

The general lesson, which will save you again later in this course: in Boot 4 a starter is what activates auto-configuration, not merely a convenient bundle of jars.

### What `KafkaTemplate` is

`KafkaTemplate<K, V>` is Spring's wrapper around the raw `KafkaProducer` client. It manages the producer's lifecycle, applies your configuration, and gives you a `send()` method.

`K` and `V` are the key and value types. You will use `KafkaTemplate<String, String>`.

The auto-configuration builds one from your `spring.kafka.producer.*` properties. You inject it; you never construct it.

### Serializers are not optional

Kafka brokers store bytes. They have no idea what a `String` is. Something has to convert your Java object into a `byte[]` before it goes on the wire, and that something is a **serializer**. You must name one for the key and one for the value:

```yaml
key-serializer: org.apache.kafka.common.serialization.StringSerializer
value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

`StringSerializer` calls `getBytes(UTF_8)`. That is the whole job. In Lesson 25 you will swap these for Avro serializers that talk to a schema registry, and every consumer will need a matching deserializer or it will read garbage.

---

## Hands-on

### 1. Create the project

Go to [start.spring.io](https://start.spring.io) and select:

| Field | Value |
|---|---|
| Project | Maven |
| Language | Java |
| Spring Boot | 4.1.0 |
| Group | `com.example.wikimedia` |
| Artifact | `wikimedia-producer` |
| Packaging | Jar |
| Java | 25 |
| Dependencies | Spring for Apache Kafka |

Generate, unzip, and put the result **next to your `docker-compose.yml`**:

```
kafka-course/
├── docker-compose.yml
└── wikimedia-producer/
    ├── mvnw
    ├── pom.xml
    └── src/
```

The `mvnw` script is the Maven Wrapper, generated for you by Spring Initializr. It downloads and runs a pinned Maven version, so everyone building the project uses the same one whether or not they have Maven installed. Use `./mvnw` rather than `mvn` from here on.

If you would rather not use the website, create the files below by hand.

### 2. `pom.xml`

The essentials. Note there is no web dependency: this lesson's application has no HTTP endpoint at all.

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

    <groupId>com.example.wikimedia</groupId>
    <artifactId>wikimedia-producer</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <properties>
        <java.version>25</java.version>
    </properties>

    <dependencies>
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

### 3. `src/main/resources/application.yml`

```yaml
spring:
  application:
    name: wikimedia-producer

  kafka:
    producer:
      # Your app runs on your machine, so it uses the brokers' host-facing listeners.
      # All three are listed so a single dead broker cannot stop you bootstrapping.
      bootstrap-servers: localhost:9092,localhost:9093,localhost:9094

      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

server:
  port: 8081
```

Three things to notice.

**`localhost:9092,9093,9094`, not `kafka-1:29092`.** This is the mirror image of the rule you learned in Lesson 06. There, inside a container, `localhost` was wrong and the container hostname was right. Here your application runs on your machine, so the published ports are right and the container hostnames would not resolve at all. Same brokers, different doors.

**All three are listed.** The client needs only one address to bootstrap, since it learns the rest of the topology from whichever broker answers. Listing all three just means no single broker's absence stops your application from starting.

**`server.port: 8081`** does nothing yet, because this application has no web server. Set it now anyway: Kafka UI is already on 8080, and from Lesson 14 this application will serve HTTP. Getting the collision out of the way before it happens is cheaper than debugging it later.

### 4. `ProducerApplication.java`

```java
package com.example.wikimedia.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
```

If you generated the project from Initializr it created this class for you, named after the artifact. Rename it to `ProducerApplication` or keep the generated name; nothing else depends on it.

### 5. `src/main/java/com/example/wikimedia/producer/kafka/WikimediaProducer.java`

The class you will grow across the whole of Part 2. Right now it does one thing.

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

    public void sendMessage(String message) {
        kafkaTemplate.send(TOPIC, message);
        log.info("Sent: {}", message);
    }
}
```

Constructor injection, no `@Autowired`, no field injection. A single constructor means Spring wires it without any annotation at all.

The topic name is a constant rather than a literal in the `send()` call, because Lessons 10 and 13 will ask you to point this class at a different topic temporarily, and one edit in one place is easier to undo.

> **Why no Lombok here?** `@RequiredArgsConstructor` and `@Slf4j` would shorten this, and you will see them used that way in plenty of projects. The convention in this course is that Lombok belongs on entities and data-carrying types, not on services and controllers, which are the two places you most want the constructor and its dependencies visible at a glance. You will meet Lombok properly on the JPA entity in Lesson 18.

### 6. Send something at startup

A throwaway class, just for these early lessons. You will delete it in Lesson 14.

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
        producer.sendMessage("hello from spring boot");
        producer.sendMessage("second message");
        producer.sendMessage("third message");
    }
}
```

`ApplicationRunner` runs once, after the context is ready. No HTTP and no scheduling: the simplest way to make code execute on startup.

### 7. Run it

```bash
cd wikimedia-producer
./mvnw spring-boot:run
```

You should see the three log lines, and then **the application exits on its own**:

```
Sent: hello from spring boot
Sent: second message
Sent: third message
```

That is expected, and worth understanding rather than worrying about. A JVM exits once no non-daemon threads remain. This application has no web server holding a thread open, and Kafka's own network thread is a daemon, so once `ApplicationRunner` returns there is nothing left to keep the process alive. Spring closes the context on the way out, which flushes the producer.

Applications in Part 3 and from Lesson 14 onward stay running, because they have a web server or a listener container. This one has neither yet.

### 8. Verify at the broker, not in your logs

Your log said "Sent". Ask Kafka:

```bash
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic wikimedia-stream \
  --from-beginning \
  --max-messages 3 \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

```
Partition:1	Offset:0	hello from spring boot
Partition:1	Offset:1	second message
Partition:1	Offset:2	third message
```

Three records at real offsets. All three on one partition, and probably not partition 0: they are keyless and fitted in a single batch, which is exactly the behaviour you measured in Lesson 04.

> If the topic already held data from Part 1, offsets will be higher and older records appear first. Use `--max-messages` generously, or delete and recreate the topic.

### 9. Where did the topic come from?

You never created `wikimedia-stream` in this lesson. If it did not already exist, the broker created it the moment you produced to it. That is **auto topic creation**, and it is on by default in this Compose setup.

Convenient, and a trap. An auto-created topic uses the broker's defaults for partition count, replication factor and retention, and gets no `min.insync.replicas` override at all, which after Lesson 06 you know is the difference between a durable topic and one that merely looks durable. A typo in a topic name also silently creates a brand-new topic instead of failing.

Check what you got:

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic wikimedia-stream
```

```
Topic: wikimedia-stream	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=1
```

Three partitions and three replicas, from the broker defaults you set in Lesson 06. And a floor of 1, which is not what you want for data you care about.

In production, auto-creation is switched off and topics are declared explicitly. That is the next lesson.

---

## Try it yourself

1. Change `TOPIC` to `typo-stream`, run, then list topics. What happened? Now imagine that typo in a consumer instead of a producer, and describe how the symptom would differ.

2. Delete the `value-serializer` line from `application.yml` and run. Read the exception carefully. Does Spring fall back to a default, and what does the answer tell you about which library is actually validating this configuration?

3. Stop `kafka-1` and run the application. Does it still start and send? Explain using the word *bootstrap*. Then start it again.

4. Point `bootstrap-servers` at `kafka-1:29092` instead, and run. The failure is immediate and total. Explain it in terms of Lesson 06's listener table, and say which single line of your Compose file decides whether that address could ever work from your machine.

---

## Common mistakes

**Depending on `org.springframework.kafka:spring-kafka` instead of `spring-boot-starter-kafka`.**
Compiles fine, then fails at startup with "required a bean of type `KafkaTemplate`". The starter brings the auto-configuration; the bare library does not.

**Using `kafka-1:29092` in `application.yml`.**
That address only resolves inside the Docker network. From your machine, use `localhost:9092`.

**Expecting the application to stay running.**
With no web server and no listener container, it exits after `ApplicationRunner` finishes. Nothing is wrong.

**Trusting `log.info("Sent")`.**
`send()` is asynchronous. That line proves the record was handed to the producer's buffer, not that any broker accepted it. Lesson 13 fixes this properly.

**Relying on auto topic creation.**
You get broker defaults, and typos become topics.

---

## Check your understanding

**1. Your application compiles, the `KafkaTemplate` import resolves, and at startup Spring says no bean of that type could be found. How is that possible?**

<details>
<summary>Reveal answer</summary>

Having a class on the classpath and having a bean of that class in the context are unrelated facts.

`spring-kafka` puts `KafkaTemplate` on the classpath, so the import resolves. But under Spring Boot 4 the code that *creates* the bean, `KafkaAutoConfiguration`, lives in the separate `spring-boot-kafka` module, pulled in by `spring-boot-starter-kafka`. Without the starter, nothing ever calls `new KafkaTemplate(...)`.

The fix is to depend on the starter. The general lesson is that in Boot 4 a starter activates auto-configuration rather than merely bundling jars.

</details>

**2. `bootstrap-servers` lists three brokers. Does the producer open connections to all three?**

<details>
<summary>Reveal answer</summary>

Eventually, but not because you listed three.

The producer contacts one of them to bootstrap and receives the full cluster metadata: every broker, every partition, and which broker leads each partition. From then on it connects directly to the leader of whichever partition it is writing to, which may well be a broker you never listed.

Listing all three only protects the bootstrap step. If you listed one broker and that broker were down at startup, the producer could not discover the cluster at all, even with the other two healthy.

</details>

**3. You produce three keyless records to a 3-partition topic and all three land on the same partition. Is the partitioner broken?**

<details>
<summary>Reveal answer</summary>

No, and you already proved this in Lesson 04.

With no key, the producer's built-in partitioner sends records to one partition until roughly `batch.size` bytes have accumulated for it, then switches. Three short records are nowhere near 16 KiB, so they all went to one partition, chosen without reference to their content.

Over millions of records the distribution evens out. It has never been round-robin per record, and the class that introduced this behaviour was removed in Kafka 4.0 in favour of it being built in.

</details>

**4. `sendMessage()` logs "Sent" and returns. The broker was unreachable. Why did nothing throw?**

<details>
<summary>Reveal answer</summary>

`kafkaTemplate.send()` is asynchronous. It serialises the record, appends it to an in-memory buffer and returns immediately, before any network activity happens. A background thread later batches the buffer and ships it.

So `send()` returning successfully means "accepted into the buffer" and nothing more. A failure surfaces later, on the `CompletableFuture` that `send()` returned and this code ignored.

Your log line therefore proves only that the method was called. This is why Lesson 13 attaches a callback, and why `acks`, which is Lesson 10, is about what the broker promises rather than what `send()` returns.

</details>

**5. Auto topic creation made `wikimedia-stream` for you, with three partitions and three replicas. Given that those are reasonable numbers, what is still wrong with it?**

<details>
<summary>Reveal answer</summary>

Two things, one visible in the output and one not.

The visible one is `min.insync.replicas=1`. After Lesson 06 you know that replication factor 3 with a floor of 1 means `acks=all` degrades silently to `acks=1` as soon as two replicas fall behind. The topic looks durable and is not.

The invisible one is that nobody decided any of this. The partition count, replication factor and retention came from whatever the broker defaults happened to be at the moment of first produce. Change a broker default six months from now and new topics quietly get different durability. And a typo produces a second topic rather than an error, so a producer can write happily to `wikimedia-strem` while every consumer sits on the correctly spelled one seeing nothing, which looks exactly like data loss.

</details>

---

## Recap

You wired Spring Boot to Kafka with `spring-boot-starter-kafka`, the starter rather than the bare library, injected a `KafkaTemplate<String, String>`, and produced three records. Then you ignored your own log output and asked the broker whether they had really arrived.

You also let Kafka auto-create your topic, which is exactly what you should not do.

**Next:** [Lesson 09: Topics as Code](09-topics-as-code.md)
