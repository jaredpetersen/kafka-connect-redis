package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.source.config.RedisSourceConfig;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisSourceConnectorIT {
  @Test
  public void versionReturnsVersion() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    assertEquals(VersionUtil.getVersion(), sourceConnector.version());
  }

  @Test
  public void taskClassReturnsTaskClass() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    assertEquals(RedisSourceTask.class, sourceConnector.taskClass());
  }

  @Test
  public void taskConfigsReturnsTaskConfigs() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.uri", "redis://localhost:6379");
    connectorConfig.put("redis.cluster.enabled", "false");

    sourceConnector.start(connectorConfig);

    final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(3);

    assertEquals(1, taskConfigs.size());
    assertEquals(connectorConfig, taskConfigs.get(0));
  }

  @Test
  public void stopDoesNothing() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();
    sourceConnector.stop();
  }

  @Test
  public void configReturnsConfigDefinition() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    assertEquals(RedisSourceConfig.CONFIG_DEF, sourceConnector.config());
  }
}
