package com.distributed.cache.controller;

import com.distributed.cache.service.DistributedCacheService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/api/cache")
public class CacheController {
    private final DistributedCacheService cacheService;

    public CacheController(DistributedCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> getValue(@PathVariable String key) {
        try {
            String value = cacheService.get(key);
            return value != null 
                ? ResponseEntity.ok(value) 
                : ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error retrieving value: " + e.getMessage());
        }
    }

    @PostMapping("/{key}")
    public ResponseEntity<Void> setValue(
            @PathVariable String key,
            @RequestBody String value,
            @RequestParam(required = false, defaultValue = "3600") long ttlSeconds) {
        try {
            cacheService.put(key, value, Duration.ofSeconds(ttlSeconds));
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/batch")
    public ResponseEntity<Void> batchSetValues(
            @RequestBody Map<String, String> keyValues,
            @RequestParam(required = false, defaultValue = "3600") long ttlSeconds) {
        try {
            cacheService.batchPut(keyValues, Duration.ofSeconds(ttlSeconds));
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> deleteValue(@PathVariable String key) {
        try {
            cacheService.delete(key);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
} 