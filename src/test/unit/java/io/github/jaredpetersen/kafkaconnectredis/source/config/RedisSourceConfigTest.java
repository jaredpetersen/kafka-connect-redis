package io.github.jaredpetersen.kafkaconnectredis.source.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RedisSourceConfigTest {
  @Test
  public void getTopicReturnsDefaultTopic() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals("redis", sinkConfig.getTopic());
  }

  @Test
  public void getTopicReturnsTopic() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("topic", "mytopic");
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals("mytopic", sinkConfig.getTopic());
  }

  @Test
  public void getRedisUriReturnsUri() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals("redis://localhost:6379", sinkConfig.getRedisUri());
  }

  @Test
  public void isRedisClusterEnabledReturnsDefaultStatus() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertFalse(sinkConfig.isRedisClusterEnabled());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void isRedisClusterEnabledReturnsStatus(boolean status) {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.cluster.enabled", status);
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals(status, sinkConfig.isRedisClusterEnabled());
  }

  @Test
  public void getRedisChannelsReturnsChannels() {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("topic", "mytopic");
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", true);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals(Arrays.asList("channel1", "channel2"), sinkConfig.getRedisChannels());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  public void isRedisChannelPatternEnabledReturnsStatus(boolean status) {
    final Map<String, Object> originalConfig = new HashMap<>();
    originalConfig.put("topic", "mytopic");
    originalConfig.put("redis.uri", "redis://localhost:6379");
    originalConfig.put("redis.channels", "channel1,channel2");
    originalConfig.put("redis.channels.pattern.enabled", status);

    final RedisSourceConfig sinkConfig = new RedisSourceConfig(originalConfig);

    assertEquals(status, sinkConfig.isRedisChannelPatternEnabled());
  }
}
