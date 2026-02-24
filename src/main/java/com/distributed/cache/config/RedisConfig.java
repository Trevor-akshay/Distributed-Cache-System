package com.distributed.cache.config;

import com.distributed.cache.hashing.ConsistentHashRing;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@Configuration
public class RedisConfig {

    @Value("${spring.redis.cluster.nodes}")
    private String clusterNodes;

    @Value("${spring.redis.password}")
    private String password;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        List<String> nodes = Arrays.asList(clusterNodes.split(","));
        RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(nodes);
        if (password != null && !password.isEmpty()) {
            clusterConfig.setPassword(password);
        }

        return new LettuceConnectionFactory(clusterConfig);
    }

    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        return template;
    }

    @Bean
    public ConsistentHashRing consistentHashRing() {
        List<String> nodes = Arrays.asList(clusterNodes.split(","));
        ConsistentHashRing hashRing = new ConsistentHashRing(100); // 100 virtual nodes per physical node
        nodes.forEach(hashRing::addNode);
        return hashRing;
    }

    @Bean
    public CircuitBreaker circuitBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofMillis(1000))
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .build();

        return CircuitBreaker.of("cacheCircuitBreaker", config);
    }
} 