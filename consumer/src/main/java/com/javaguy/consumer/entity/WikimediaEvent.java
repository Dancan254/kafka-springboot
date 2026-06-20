package com.javaguy.consumer.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "wikimedia_events", indexes = {
        @Index(name = "idx_wiki", columnList = "wiki"),
        @Index(name = "idx_event_type", columnList = "event_type"),
        @Index(name = "idx_processed_at", columnList = "processed_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WikimediaEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // "edit", "new", "log", "categorize"
    @Column(name = "event_type", nullable = false)
    private String type;

    @Column(nullable = false)
    private String title;

    // "user" is a reserved SQL keyword — stored as "editor"
    @Column(name = "editor")
    private String username;

    private boolean bot;

    private Integer namespace;

    private String wiki;

    @Column(name = "server_name")
    private String serverName;

    @Column(name = "event_timestamp")
    private Long eventTimestamp;

    @Column(columnDefinition = "TEXT")
    private String comment;

    // Kafka provenance — useful for replay and lag debugging
    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
