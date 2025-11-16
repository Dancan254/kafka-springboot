package com.javaguy.producer.controller;

import com.javaguy.producer.stream.WikimediaStreamConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/wikimedia")
@RequiredArgsConstructor
public class WikimediaController {

    private final WikimediaStreamConsumer wikimediaStreamConsumer;

    @GetMapping
    public  void startPublishing() {
        wikimediaStreamConsumer.consumeStreamAndPublish();
    }
}
