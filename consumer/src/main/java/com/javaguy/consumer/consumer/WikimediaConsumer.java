package com.javaguy.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javaguy.consumer.dto.WikimediaEventDto;
import com.javaguy.consumer.entity.WikimediaEvent;
import com.javaguy.consumer.repository.WikimediaEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class WikimediaConsumer {

    private final WikimediaEventRepository repository;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "wikimedia-stream",
            groupId = "wikimedia-consumer-group"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        WikimediaEventDto dto = parse(record);

        WikimediaEvent event = WikimediaEvent.builder()
                .type(dto.type())
                .title(dto.title())
                .username(dto.user())
                .bot(dto.bot())
                .namespace(dto.namespace())
                .wiki(dto.wiki())
                .serverName(dto.serverName())
                .eventTimestamp(dto.timestamp())
                .comment(dto.comment())
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .processedAt(LocalDateTime.now())
                .build();

        repository.save(event);

        log.info("Saved | partition={} offset={} type={} wiki={} title='{}'",
                record.partition(), record.offset(), dto.type(), dto.wiki(), dto.title());

        // Commit offset only after the record is durably written to the database.
        // If save() throws (e.g. DB temporarily down), the exception propagates to
        // DefaultErrorHandler, which retries with exponential backoff before DLT routing.
        acknowledgment.acknowledge();
    }

    private WikimediaEventDto parse(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), WikimediaEventDto.class);
        } catch (JsonProcessingException e) {
            // Malformed JSON will never parse correctly no matter how many retries.
            // Wrapping in IllegalArgumentException triggers the non-retryable path in
            // DefaultErrorHandler and routes the record straight to wikimedia-stream.dlt.
            throw new IllegalArgumentException(
                    "Unparseable Wikimedia event [partition=%d offset=%d]: %s"
                            .formatted(record.partition(), record.offset(), e.getMessage()), e);
        }
    }
}
