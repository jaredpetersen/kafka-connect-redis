package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
class RedisSinkTaskIT {
  @Container
  private static final RedisContainer REDIS_STANDALONE = new RedisContainer();

  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static String REDIS_STANDALONE_URI;
  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static StatefulRedisConnection<String, String> REDIS_STANDALONE_CONNECTION;
  private static RedisCommands<String, String> REDIS_STANDALONE_COMMANDS;

  private static String REDIS_CLUSTER_URI;
  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterConnection<String, String> REDIS_CLUSTER_CONNECTION;
  private static RedisClusterCommands<String, String> REDIS_CLUSTER_COMMANDS;

  private static final Schema REDIS_SET_COMMAND_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSetCommand")
    .field("key", SchemaBuilder.STRING_SCHEMA)
    .field("value", SchemaBuilder.STRING_SCHEMA)
    .field("expiration", SchemaBuilder.struct()
      .field("type", SchemaBuilder.STRING_SCHEMA)
      .field("time", SchemaBuilder.INT64_SCHEMA)
      .optional())
    .field("condition", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
    .required()
    .build();

  @BeforeAll
  static void beforeAll() {
    REDIS_STANDALONE_URI = REDIS_STANDALONE.getUri();
    REDIS_STANDALONE_CLIENT = RedisClient.create(REDIS_STANDALONE_URI);
    REDIS_STANDALONE_CONNECTION = REDIS_STANDALONE_CLIENT.connect();
    REDIS_STANDALONE_COMMANDS = REDIS_STANDALONE_CONNECTION.sync();

    REDIS_CLUSTER_URI = REDIS_CLUSTER.getUri();
    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(REDIS_CLUSTER_URI);
    REDIS_CLUSTER_CONNECTION = REDIS_CLUSTER_CLIENT.connect();
    REDIS_CLUSTER_COMMANDS = REDIS_CLUSTER_CONNECTION.sync();
  }

  @AfterEach
  void afterEach() {
    REDIS_STANDALONE_COMMANDS.flushall();
    REDIS_CLUSTER_COMMANDS.flushall();
  }

  @AfterAll
  static void afterAll() {
    REDIS_STANDALONE_CONNECTION.close();
    REDIS_STANDALONE_CLIENT.shutdown();

    REDIS_CLUSTER_CONNECTION.close();
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  @Test
  void versionReturnsVersion() {
    final RedisSinkTask sinkTask = new RedisSinkTask();

    assertEquals(VersionUtil.getVersion(), sinkTask.version());
  }

  @Test
  void putRecordsAppliesCommandsToStandalone() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");

    // Set up records to write
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Struct value = new Struct(valueSchema)
      .put("key", "{user.1}.username")
      .put("value", "jetpackmelon22");
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final List<SinkRecord> sinkRecords = Collections.singletonList(sinkRecord);

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get("{user.1}.username"));
  }

  @Test
  void putRecordsAppliesCommandsToCluster() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");

    // Set up records to write
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Struct value = new Struct(valueSchema)
      .put("key", "{user.1}.username")
      .put("value", "jetpackmelon22");
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final List<SinkRecord> sinkRecords = Arrays.asList(sinkRecord);

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get("{user.1}.username"));
  }

  @Test
  void putEmptyRecordsDoesNothingToStandalone() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");

    // Set up records to write
    final List<SinkRecord> sinkRecords = Collections.emptyList();

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    assertEquals(0L, REDIS_STANDALONE_COMMANDS.dbsize());
  }

  @Test
  void putEmptyRecordsDoesNothingToCluster() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");

    // Set up records to write
    final List<SinkRecord> sinkRecords = Collections.emptyList();

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    assertEquals(0L, REDIS_CLUSTER_COMMANDS.dbsize());
  }

  @Test
  void startThrowsConnectExceptionForInvalidConfig() {
    final RedisSinkTask sinkTask = new RedisSinkTask();

    final Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("redis.cluster.enabled", "false");

    final ConnectException thrown = assertThrows(ConnectException.class, () -> sinkTask.start(connectorConfig));
    assertEquals("task configuration error", thrown.getMessage());
  }

  @Test
  void stopClosesStandalone() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.stop();

    // Can't actually verify connections are closed since lettuce does what it wants
  }

  @Test
  void stopClosesCluster() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.stop();

    // Can't actually verify connections are closed since lettuce does what it wants
  }
}
