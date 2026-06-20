package com.javaguy.consumer.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Routes a failed record to wikimedia-stream.dlt on the SAME partition as the
     * source so per-partition ordering is preserved on the DLT side.
     * <p>
     * Spring Kafka automatically appends the following headers to every DLT record:
     * <ol>
     * <li>{@code kafka_dlt-original-topic} — original topic name</li>
     * <li>{@code kafka_dlt-original-partition} — original partition (int bytes)</li>
     * <li>{@code kafka_dlt-original-offset} — original offset (long bytes)</li>
     * <li>{@code kafka_dlt-original-timestamp} — original timestamp (long bytes)</li>
     * <li>{@code kafka_dlt-exception-fqcn} — fully qualified exception class name</li>
     * <li>{@code kafka_dlt-exception-message} — exception message string</li>
     * <li>{@code kafka_dlt-exception-cause-fqcn} — root cause class name (if present)</li>
     * </ol>
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(
                        record.topic() + ".dlt",
                        record.partition()));
    }

    /**
     * {@code DefaultErrorHandler} replaces the old {@code SeekToCurrentErrorHandler} (removed in Spring Kafka 3.x).
     * <p>
     * Retry schedule (per record, not per batch):
     * <ol>
     * <li>Attempt 1 — immediate</li>
     * <li>Attempt 2 — wait 1,000 ms</li>
     * <li>Attempt 3 — wait 2,000 ms</li>
     * <li>Attempt 4 — wait 4,000 ms &rarr; routed to DLT</li>
     * </ol>
     * <p>
     * {@code ExponentialBackOff} tracks theoretical elapsed time across {@code nextBackOff()} calls.
     * After three intervals (1,000 + 2,000 + 4,000 = 7,000 ms), the elapsed total reaches
     * {@code maxElapsedTime} and the next call returns {@code BackOffExecution.STOP}, triggering DLT routing.
     * <p>
     * Non-retryable exceptions skip backoff entirely and go straight to the DLT—retrying
     * will never fix data-level issues like deserialization errors or schema violations.
     * </p>
     */
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        var backoff = new ExponentialBackOff(1_000L, 2.0);
        backoff.setMaxInterval(10_000L);
        backoff.setMaxElapsedTime(7_000L); // stops after 3 retry intervals: 1s + 2s + 4s

        var handler = new DefaultErrorHandler(recoverer, backoff);

        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    /**
     * Defines the listener container factory used by all @KafkaListener methods.
     * Declaring this bean ourselves (instead of relying on Spring Boot's auto-configuration)
     * lets us wire in the custom error handler. The ConsumerFactory is still
     * auto-configured from spring.kafka.consumer.* properties in application.yml.
     * <p>
     * Note: spring.kafka.listener.* YAML properties apply ONLY to Spring Boot's
     * auto-configured factory. Since we own the factory here, those properties are
     * ignored — all listener-level settings must be set on this factory directly.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        return factory;
    }
}
