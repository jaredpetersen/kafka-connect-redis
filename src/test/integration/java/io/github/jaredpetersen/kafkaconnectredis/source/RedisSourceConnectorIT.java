package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.source.config.RedisSourceConfig;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    connectorConfig.put("redis.channels", "channel1,channel2,channel3");
    connectorConfig.put("redis.channels.pattern.enabled", "false");

    sourceConnector.start(connectorConfig);

    final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(1);

    assertEquals(1, taskConfigs.size());
    assertEquals(connectorConfig, taskConfigs.get(0));
  }

  @Test
  public void taskConfigsReturnsPartitionedTaskConfigs() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.uri", "redis://localhost:6379");
    connectorConfig.put("redis.cluster.enabled", "false");
    connectorConfig.put("redis.channels", "channel1,channel2,channel3");
    connectorConfig.put("redis.channels.pattern.enabled", "false");

    final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
    expectedPartitionedConnectorConfigA.put("redis.uri", "redis://localhost:6379");
    expectedPartitionedConnectorConfigA.put("redis.cluster.enabled", "false");
    expectedPartitionedConnectorConfigA.put("redis.channels", "channel1,channel2");
    expectedPartitionedConnectorConfigA.put("redis.channels.pattern.enabled", "false");

    final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
    expectedPartitionedConnectorConfigB.put("redis.uri", "redis://localhost:6379");
    expectedPartitionedConnectorConfigB.put("redis.cluster.enabled", "false");
    expectedPartitionedConnectorConfigB.put("redis.channels", "channel3");
    expectedPartitionedConnectorConfigB.put("redis.channels.pattern.enabled", "false");

    sourceConnector.start(connectorConfig);

    final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(2);

    assertEquals(2, taskConfigs.size());
    assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
    assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
  }

  @Test
  public void taskConfigsReturnsPartitionedTaskConfigsWithExcessiveMaxTask() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.uri", "redis://localhost:6379");
    connectorConfig.put("redis.cluster.enabled", "false");
    connectorConfig.put("redis.channels", "channel1,channel2,channel3");
    connectorConfig.put("redis.channels.pattern.enabled", "false");

    final Map<String, String> expectedPartitionedConnectorConfigA = new HashMap<>();
    expectedPartitionedConnectorConfigA.put("redis.uri", "redis://localhost:6379");
    expectedPartitionedConnectorConfigA.put("redis.cluster.enabled", "false");
    expectedPartitionedConnectorConfigA.put("redis.channels", "channel1");
    expectedPartitionedConnectorConfigA.put("redis.channels.pattern.enabled", "false");

    final Map<String, String> expectedPartitionedConnectorConfigB = new HashMap<>();
    expectedPartitionedConnectorConfigB.put("redis.uri", "redis://localhost:6379");
    expectedPartitionedConnectorConfigB.put("redis.cluster.enabled", "false");
    expectedPartitionedConnectorConfigB.put("redis.channels", "channel2");
    expectedPartitionedConnectorConfigB.put("redis.channels.pattern.enabled", "false");

    final Map<String, String> expectedPartitionedConnectorConfigC = new HashMap<>();
    expectedPartitionedConnectorConfigC.put("redis.uri", "redis://localhost:6379");
    expectedPartitionedConnectorConfigC.put("redis.cluster.enabled", "false");
    expectedPartitionedConnectorConfigC.put("redis.channels", "channel3");
    expectedPartitionedConnectorConfigC.put("redis.channels.pattern.enabled", "false");

    sourceConnector.start(connectorConfig);

    final List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(40);

    assertEquals(3, taskConfigs.size());
    assertEquals(expectedPartitionedConnectorConfigA, taskConfigs.get(0));
    assertEquals(expectedPartitionedConnectorConfigB, taskConfigs.get(1));
    assertEquals(expectedPartitionedConnectorConfigC, taskConfigs.get(2));
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
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();
    sourceConnector.stop();
  }

  @Test
  public void configReturnsConfigDefinition() {
    final RedisSourceConnector sourceConnector = new RedisSourceConnector();

    assertEquals(RedisSourceConfig.CONFIG_DEF, sourceConnector.config());
  }
}
