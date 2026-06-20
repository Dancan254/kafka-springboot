package com.javaguy.consumer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Maps only the fields we care about from the Wikimedia Recent Changes event payload.
 * All unrecognised fields are silently discarded — the raw JSON is preserved in the entity
 * for replay and debugging without bloating the relational schema.
 *
 * Sample Wikimedia event shape:
 * {
 *   "type":        "edit" | "new" | "log" | "categorize",
 *   "title":       "Wikipedia article title",
 *   "user":        "editor username or IP",
 *   "bot":         false,
 *   "namespace":   0,
 *   "wiki":        "enwiki",
 *   "server_name": "en.wikipedia.org",
 *   "timestamp":   1704067200,
 *   "comment":     "edit summary"
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikimediaEventDto(
        String type,
        String title,
        String user,
        boolean bot,
        Integer namespace,
        String wiki,
        @JsonProperty("server_name") String serverName,
        Long timestamp,
        String comment
) {}
