package com.javaguy.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class WikimediaDltConsumer {

    /**
     * Consumes from the dead-letter topic. Every record here was published by
     * DeadLetterPublishingRecoverer after the main consumer exhausted its retries.
     *
     * In production you would typically:
     *   1. Persist the failed record to a dead-letter store (DB, S3, etc.)
     *   2. Fire an alert (PagerDuty, Slack, metric increment)
     *   3. Provide an admin endpoint to replay or discard the record
     *
     * This implementation logs all DLT headers so the failure is fully observable.
     */
    @KafkaListener(
            topics = "wikimedia-stream.dlt",
            groupId = "wikimedia-dlt-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.error(
                "[DLT] partition={} offset={} | originalTopic={} originalPartition={} " +
                "originalOffset={} | exception={} cause='{}' | value='{}'",
                record.partition(),
                record.offset(),
                stringHeader(record, "kafka_dlt-original-topic"),
                intHeader(record, "kafka_dlt-original-partition"),
                longHeader(record, "kafka_dlt-original-offset"),
                stringHeader(record, "kafka_dlt-exception-fqcn"),
                stringHeader(record, "kafka_dlt-exception-message"),
                record.value()
        );

        acknowledgment.acknowledge();
    }

    // DLT headers for topic/exception info are UTF-8 strings
    private String stringHeader(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? "n/a" : new String(h.value(), StandardCharsets.UTF_8);
    }

    // Partition is stored as a 4-byte big-endian int
    private int intHeader(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? -1 : ByteBuffer.wrap(h.value()).getInt();
    }

    // Offset is stored as an 8-byte big-endian long
    private long longHeader(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? -1L : ByteBuffer.wrap(h.value()).getLong();
    }
}
