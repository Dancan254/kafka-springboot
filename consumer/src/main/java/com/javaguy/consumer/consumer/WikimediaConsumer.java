package com.javaguy.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WikimediaConsumer {

    @KafkaListener(topics = "wikimedia-stream", groupId = "consumer-group")
    public void consume(String message) {
        log.info("Received message='{}'", message);
    }
}
