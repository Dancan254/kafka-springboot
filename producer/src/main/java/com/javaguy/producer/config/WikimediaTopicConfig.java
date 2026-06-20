package com.javaguy.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class WikimediaTopicConfig {

    @Bean
    public NewTopic wikimediaStreamTopic() {
        return TopicBuilder
                .name("wikimedia-stream")
                // 3 partitions → up to 3 consumers in the same group can read in parallel,
                // each owning one partition. Adding more partitions later requires a rebalance
                // but does NOT require downtime.
                .partitions(3)
                // RF=3 means each partition has 1 leader + 2 follower replicas spread across
                // the 3 brokers. The cluster tolerates losing 1 broker without data loss.
                .replicas(3)
                // A produce request only succeeds when at least 2 of the 3 replicas have
                // acknowledged the write (pairs with producer acks=all).
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                // Retain events for 7 days (604_800_000 ms). After this window, Kafka
                // deletes old log segments — not individual messages.
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                // Store up to 10 GiB per partition before triggering size-based cleanup.
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "10737418240")
                // Match the producer compression; avoids re-compression on the broker.
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .build();
    }
}
