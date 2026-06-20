package com.javaguy.consumer.controller;

import com.javaguy.consumer.entity.WikimediaEvent;
import com.javaguy.consumer.repository.WikimediaEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/wikimedia/events")
@RequiredArgsConstructor
public class WikimediaEventController {

    private final WikimediaEventRepository repository;

    /**
     * Paginated list of all consumed events, newest first.
     * GET /api/v1/wikimedia/events?page=0&size=20
     */
    @GetMapping
    public Page<WikimediaEvent> getAll(
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return repository.findAll(pageable);
    }

    /**
     * The 10 most recently consumed events — useful for a live dashboard.
     * GET /api/v1/wikimedia/events/recent
     */
    @GetMapping("/recent")
    public List<WikimediaEvent> getRecent() {
        return repository.findTop10ByOrderByProcessedAtDesc();
    }

    /**
     * Events filtered by wiki (e.g. "enwiki", "dewiki", "commonswiki").
     * GET /api/v1/wikimedia/events/wiki/enwiki?page=0&size=20
     */
    @GetMapping("/wiki/{wiki}")
    public Page<WikimediaEvent> getByWiki(
            @PathVariable String wiki,
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return repository.findByWiki(wiki, pageable);
    }

    /**
     * Events filtered by type: "edit", "new", "log", "categorize".
     * GET /api/v1/wikimedia/events/type/edit?page=0&size=20
     */
    @GetMapping("/type/{type}")
    public Page<WikimediaEvent> getByType(
            @PathVariable String type,
            @PageableDefault(size = 20, sort = "processedAt", direction = Sort.Direction.DESC)
            Pageable pageable) {
        return repository.findByType(type, pageable);
    }

    /**
     * Aggregated stats across all consumed events.
     * GET /api/v1/wikimedia/events/stats
     *
     * Example response:
     * {
     *   "totalEvents": 1500,
     *   "botEdits":    320,
     *   "humanEdits":  1180,
     *   "byWiki":  { "enwiki": 800, "dewiki": 300, ... },
     *   "byType":  { "edit": 1200, "new": 250, "log": 50 }
     * }
     */
    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        long total    = repository.count();
        long botEdits = repository.countByBot(true);

        Map<String, Long> byWiki = repository.countGroupedByWiki().stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Long) row[1]
                ));

        Map<String, Long> byType = repository.countGroupedByType().stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Long) row[1]
                ));

        return Map.of(
                "totalEvents", total,
                "botEdits",    botEdits,
                "humanEdits",  total - botEdits,
                "byWiki",      byWiki,
                "byType",      byType
        );
    }
}
