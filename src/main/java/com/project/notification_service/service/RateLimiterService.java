package com.project.notification_service.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Service
public class RateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private static final int MAX_REQUESTS = 75;
    private static final long WINDOW_IN_MS = TimeUnit.HOURS.toMillis(1);

    // The Lua Script: Drops old, counts current, adds new if allowed.
    private static final String LUA_SCRIPT = 
        "redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[2]) " +
        "local current_count = redis.call('ZCARD', KEYS[1]) " +
        "if current_count >= tonumber(ARGV[3]) then " +
        "   return 0 " +
        "end " +
        "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1]) " +
        "redis.call('EXPIRE', KEYS[1], 3600) " +
        "return 1 ";

    public RateLimiterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean isAllowed(String userId) {
        String redisKey = "rate_limit:user:" + userId;
        long now = Instant.now().toEpochMilli();
        long windowStart = now - WINDOW_IN_MS;

        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(LUA_SCRIPT);
        script.setResultType(Long.class);

        // Execute the script atomically in Redis
        Long result = redisTemplate.execute(
            script, 
            Collections.singletonList(redisKey), 
            String.valueOf(now), 
            String.valueOf(windowStart), 
            String.valueOf(MAX_REQUESTS)
        );

        return result != null && result == 1L;
    }
}