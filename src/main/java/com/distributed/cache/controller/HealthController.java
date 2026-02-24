package com.distributed.cache.controller;

import com.distributed.cache.hashing.ConsistentHashRing;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/health")
public class HealthController {
    private final RedisConnectionFactory redisConnectionFactory;
    private final ConsistentHashRing hashRing;

    public HealthController(RedisConnectionFactory redisConnectionFactory, ConsistentHashRing hashRing) {
        this.redisConnectionFactory = redisConnectionFactory;
        this.hashRing = hashRing;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> healthInfo = new HashMap<>();
        
        try {
            // Check Redis connection
            redisConnectionFactory.getConnection().ping();
            healthInfo.put("redis", "UP");
            
            // Get cluster nodes
            Set<String> nodes = hashRing.getAllNodes();
            healthInfo.put("clusterNodes", nodes);
            healthInfo.put("nodeCount", nodes.size());
            
            // Overall status
            healthInfo.put("status", "UP");
            
            return ResponseEntity.ok(healthInfo);
        } catch (Exception e) {
            healthInfo.put("status", "DOWN");
            healthInfo.put("error", e.getMessage());
            return ResponseEntity.status(503).body(healthInfo);
        }
    }
} 