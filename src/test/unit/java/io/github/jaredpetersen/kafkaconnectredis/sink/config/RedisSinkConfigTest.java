package io.github.jaredpetersen.kafkaconnectredis.sink.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisSinkConfigTest {
  @Test
  public void getRedisUriReturnsUri() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");

    final RedisSinkConfig sinkConfig = new RedisSinkConfig(originalConfig);

    assertEquals("redis://localhost:6379", sinkConfig.getRedisUri());
  }

  @Test
  public void isRedisClusterEnabledReturnsDefaultStatus() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");

    final RedisSinkConfig sinkConfig = new RedisSinkConfig(originalConfig);

    assertEquals(false, sinkConfig.isRedisClusterEnabled());
  }

  @Test
  public void isRedisClusterEnabledReturnsStatus() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.cluster.enabled", true);

    final RedisSinkConfig sinkConfig = new RedisSinkConfig(originalConfig);

    assertEquals(true, sinkConfig.isRedisClusterEnabled());
  }
}
