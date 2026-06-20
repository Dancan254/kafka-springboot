package com.javaguy.producer.stream;

import com.javaguy.producer.producer.WikimediaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
@Slf4j
public class WikimediaStreamConsumer {

    private final WebClient webClient;
    private final WikimediaProducer producer;

    public WikimediaStreamConsumer(WebClient.Builder webClientBuilder, WikimediaProducer producer) {
        this.webClient = webClientBuilder.baseUrl("https://stream.wikimedia.org/v2").build();
        this.producer = producer;
    }

    public void consumeStreamAndPublish() {
        // Use the SSE decoder so WebClient parses the "data: {...}" framing and
        // hands us the raw JSON payload — no SSE protocol noise reaches Kafka.
        webClient.get()
                .uri("/stream/recentchange")
                .retrieve()
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .filter(event -> event.data() != null && !event.data().isBlank())
                .subscribe(
                        event -> producer.sendMessage(event.data()),
                        err -> log.error("SSE stream error: {}", err.getMessage())
                );
    }
}
