package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.sink.config.RedisSinkConfig;
import io.github.jaredpetersen.kafkaconnectredis.source.RedisSourceConnector;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RedisSinkConnectorIT {
  @Test
  public void versionReturnsVersion() {
    final RedisSinkConnector sinkConnector = new RedisSinkConnector();

    assertEquals(VersionUtil.getVersion(), sinkConnector.version());
  }

  @Test
  public void taskClassReturnsTaskClass() {
    final RedisSinkConnector sinkConnector = new RedisSinkConnector();

    assertEquals(RedisSinkTask.class, sinkConnector.taskClass());
  }

  @Test
  public void taskConfigsReturnsTaskConfigs() {
    final RedisSinkConnector sinkConnector = new RedisSinkConnector();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.uri", "redis://localhost:6379");
    connectorConfig.put("redis.cluster.enabled", "false");

    sinkConnector.start(connectorConfig);

    final List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(3);

    assertEquals(3, taskConfigs.size());
    assertEquals(connectorConfig, taskConfigs.get(0));
    assertEquals(connectorConfig, taskConfigs.get(1));
    assertEquals(connectorConfig, taskConfigs.get(2));
  }

  @Test
  public void startThrowsConnectExceptionForInvalidConfig() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.uri", "redis://localhost:6379");

    final ConnectException thrown = assertThrows(ConnectException.class, () -> sourceConnector.start(connectorConfig));
    assertEquals("connector configuration error", thrown.getMessage());
  }

  @Test
  public void stopDoesNothing() {
    final RedisSinkConnector sinkConnector = new RedisSinkConnector();
    sinkConnector.stop();
  }

  @Test
  public void configReturnsConfigDefinition() {
    final RedisSinkConnector sinkConnector = new RedisSinkConnector();

    assertEquals(RedisSinkConfig.CONFIG_DEF, sinkConnector.config());
  }
}
