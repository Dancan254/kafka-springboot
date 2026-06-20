package com.javaguy.consumer.config;

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
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "10737418240")
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .build();
    }

    /**
     * Dead-letter topic: same partition count as the source topic so
     * DeadLetterPublishingRecoverer can route to the matching partition
     * (preserving per-key ordering on the DLT side).
     *
     * Retention is 30 days — failed messages need a longer investigation window
     * than the live stream. Size-based cleanup is disabled (-1) so nothing is
     * evicted before the time window expires.
     */
    @Bean
    public NewTopic wikimediaStreamDltTopic() {
        return TopicBuilder
                .name("wikimedia-stream.dlt")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")  // 30 days
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "-1")       // time-only eviction
                .build();
    }
}
