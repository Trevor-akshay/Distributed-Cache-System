package com.distributed.cache.service;

import com.distributed.cache.hashing.ConsistentHashRing;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class DistributedCacheService {
    private final StringRedisTemplate redisTemplate;
    private final ConsistentHashRing hashRing;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final MeterRegistry meterRegistry;
    private final Map<String, StringRedisTemplate> nodeTemplates;
    private final Timer operationTimer;

    public DistributedCacheService(StringRedisTemplate redisTemplate,
                                 ConsistentHashRing hashRing,
                                 CircuitBreaker circuitBreaker,
                                 MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.hashRing = hashRing;
        this.circuitBreaker = circuitBreaker;
        this.meterRegistry = meterRegistry;
        this.nodeTemplates = new ConcurrentHashMap<>();
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(100))
            .build();
        this.retry = Retry.of("cacheRetry", retryConfig);
        
        // Initialize metrics
        this.operationTimer = meterRegistry.timer("cache.operation.latency");
    }

    public void put(String key, String value, Duration ttl) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String primaryNode = hashRing.getNode(key);
            if (primaryNode == null) {
                throw new IllegalStateException("No nodes available in the cluster");
            }

            StringRedisTemplate primaryTemplate = getNodeTemplate(primaryNode);
            
            circuitBreaker.executeSupplier(() -> 
                retry.executeSupplier(() -> {
                    primaryTemplate.opsForValue().set(key, value, ttl);
                    return null;
                })
            );

            List<String> backupNodes = hashRing.getBackupNodes(primaryNode, 2);
            for (String backupNode : backupNodes) {
                StringRedisTemplate backupTemplate = getNodeTemplate(backupNode);
                backupTemplate.opsForValue().set(key, value, ttl);
            }

            meterRegistry.counter("cache.operations", "operation", "put", "status", "success").increment();
        } catch (Exception e) {
            meterRegistry.counter("cache.operations", "operation", "put", "status", "error").increment();
            throw e;
        } finally {
            sample.stop(operationTimer);
        }
    }

    public String get(String key) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String primaryNode = hashRing.getNode(key);
            if (primaryNode == null) {
                throw new IllegalStateException("No nodes available in the cluster");
            }

            StringRedisTemplate primaryTemplate = getNodeTemplate(primaryNode);
            
            return circuitBreaker.executeSupplier(() ->
                retry.executeSupplier(() -> {
                    String value = primaryTemplate.opsForValue().get(key);
                    if (value == null) {
                        meterRegistry.counter("cache.misses").increment();
                    } else {
                        meterRegistry.counter("cache.hits").increment();
                    }
                    return value;
                })
            );
        } catch (Exception e) {
            meterRegistry.counter("cache.operations", "operation", "get", "status", "error").increment();
            throw e;
        } finally {
            sample.stop(operationTimer);
        }
    }

    public void delete(String key) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String primaryNode = hashRing.getNode(key);
            if (primaryNode == null) {
                throw new IllegalStateException("No nodes available in the cluster");
            }

            StringRedisTemplate primaryTemplate = getNodeTemplate(primaryNode);
            
            circuitBreaker.executeSupplier(() ->
                retry.executeSupplier(() -> {
                    primaryTemplate.delete(key);
                    return null;
                })
            );

            List<String> backupNodes = hashRing.getBackupNodes(primaryNode, 2);
            for (String backupNode : backupNodes) {
                StringRedisTemplate backupTemplate = getNodeTemplate(backupNode);
                backupTemplate.delete(key);
            }

            meterRegistry.counter("cache.operations", "operation", "delete", "status", "success").increment();
        } catch (Exception e) {
            meterRegistry.counter("cache.operations", "operation", "delete", "status", "error").increment();
            throw e;
        } finally {
            sample.stop(operationTimer);
        }
    }

    public void batchPut(Map<String, String> keyValues, Duration ttl) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                for (Map.Entry<String, String> entry : keyValues.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String primaryNode = hashRing.getNode(key);
                    
                    if (primaryNode != null) {
                        StringRedisTemplate primaryTemplate = getNodeTemplate(primaryNode);
                        primaryTemplate.opsForValue().set(key, value, ttl);
                        
                        List<String> backupNodes = hashRing.getBackupNodes(primaryNode, 2);
                        for (String backupNode : backupNodes) {
                            StringRedisTemplate backupTemplate = getNodeTemplate(backupNode);
                            backupTemplate.opsForValue().set(key, value, ttl);
                        }
                    }
                }
                return null;
            });
            
            meterRegistry.counter("cache.operations", "operation", "batchPut", "status", "success").increment();
        } catch (Exception e) {
            meterRegistry.counter("cache.operations", "operation", "batchPut", "status", "error").increment();
            throw e;
        } finally {
            sample.stop(operationTimer);
        }
    }

    private StringRedisTemplate getNodeTemplate(String node) {
        return nodeTemplates.computeIfAbsent(node, n -> {
            // In a real implementation, this would create a new Redis template
            // with the specific node's connection details
            return redisTemplate;
        });
    }
} 