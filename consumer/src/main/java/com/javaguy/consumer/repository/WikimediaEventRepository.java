package com.javaguy.consumer.repository;

import com.javaguy.consumer.entity.WikimediaEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface WikimediaEventRepository extends JpaRepository<WikimediaEvent, Long> {

    Page<WikimediaEvent> findByWiki(String wiki, Pageable pageable);

    Page<WikimediaEvent> findByType(String type, Pageable pageable);

    List<WikimediaEvent> findTop10ByOrderByProcessedAtDesc();

    long countByBot(boolean bot);

    @Query("SELECT e.wiki, COUNT(e) FROM WikimediaEvent e GROUP BY e.wiki ORDER BY COUNT(e) DESC")
    List<Object[]> countGroupedByWiki();

    @Query("SELECT e.type, COUNT(e) FROM WikimediaEvent e GROUP BY e.type ORDER BY COUNT(e) DESC")
    List<Object[]> countGroupedByType();
}
