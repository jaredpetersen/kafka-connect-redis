package io.github.jaredpetersen.kafkaconnectredis;

import io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkTask;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import reactor.test.StepVerifier;

import java.util.*;

@Testcontainers
public class RedisSinkTaskIT {
  private static final Schema REDIS_SET_COMMAND_SCHEMA = SchemaBuilder.struct()
      .field("command", SchemaBuilder.STRING_SCHEMA)
      .field("payload", SchemaBuilder.struct()
          .field("key", SchemaBuilder.STRING_SCHEMA)
          .field("value", SchemaBuilder.STRING_SCHEMA)
          .field("expiration", SchemaBuilder.struct()
              .field("type", SchemaBuilder.STRING_SCHEMA)
              .field("time", SchemaBuilder.INT64_SCHEMA)
              .optional())
          .field("condition", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
          .required()
          .build())
      .required();

  @Container
  private final GenericContainer redisStandalone = new GenericContainer(DockerImageName.parse("redis:6"))
      .withExposedPorts(6379)
      .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));;

  @Container
  private final GenericContainer redisCluster = new GenericContainer(DockerImageName.parse("redis:6"))
      .withCopyFileToContainer(MountableFile.forClasspathResource("redis/redis-cluster.conf"), "/data/redis.conf")
      .withCopyFileToContainer(MountableFile.forClasspathResource("redis/nodes-cluster.conf"), "/data/nodes.conf")
      .withCommand("redis-server", "/data/redis.conf")
      .withExposedPorts(6379)
      .waitingFor(Wait.forLogMessage(".*Cluster state changed: ok*\\n", 1));

  private String redisStandaloneUri;
  private RedisClient redisStandaloneClient;
  private StatefulRedisConnection<String, String> statefulRedisConnection;
  private RedisReactiveCommands<String, String> redisStandaloneCommands;

  private String redisClusterUri;
  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterConnection<String, String> statefulRedisClusterConnection;
  private RedisClusterReactiveCommands<String, String> redisClusterCommands;

  @BeforeEach
  public void setup() {
    this.redisStandaloneUri = "redis://" + redisStandalone.getHost() + ":" + redisStandalone.getFirstMappedPort();
    this.redisStandaloneClient = RedisClient.create(redisStandaloneUri);
    this.statefulRedisConnection = this.redisStandaloneClient.connect();
    this.redisStandaloneCommands = this.statefulRedisConnection.reactive();

    this.redisClusterUri = "redis://" + redisStandalone.getHost() + ":" + redisCluster.getFirstMappedPort();
    this.redisClusterClient = RedisClusterClient.create(redisClusterUri);
    this.statefulRedisClusterConnection = this.redisClusterClient.connect();
    this.redisClusterCommands = this.statefulRedisClusterConnection.reactive();
  }

  @AfterEach
  public void cleanup() {
    this.redisStandaloneCommands.flushall().block();
    this.statefulRedisConnection.close();
    this.redisStandaloneClient.shutdown();

    this.redisClusterCommands.flushall().block();
    this.statefulRedisClusterConnection.close();
//    this.redisClusterClient.shutdown();
  }

  @Test
  public void putRecordsAppliesCommandsToStandalone() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", redisStandaloneUri);
    config.put("redis.cluster.enabled", "false");

    // Set up records to write
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Struct value = new Struct(valueSchema)
        .put("command", "SET")
        .put("payload", new Struct(valueSchema.field("payload").schema())
            .put("key", "{user.1}.username")
            .put("value", "jetpackmelon22"));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final List<SinkRecord> sinkRecords = Arrays.asList(sinkRecord);

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    StepVerifier
        .create(redisStandaloneCommands.get("{user.1}.username"))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }

  @Test
  public void putRecordsAppliesCommandsToCluster() {
    // Set up task config
    final Map<String, String> config = new HashMap<>();
    config.put("redis.uri", redisClusterUri);
    config.put("redis.cluster.enabled", "true");

    // Set up records to write
    final String topic = "rediscommands";
    final int partition = 0;
    final Schema keySchema = null;
    final Object key = null;
    final Schema valueSchema = REDIS_SET_COMMAND_SCHEMA;
    final Struct value = new Struct(valueSchema)
        .put("command", "SET")
        .put("payload", new Struct(valueSchema.field("payload").schema())
            .put("key", "{user.1}.username")
            .put("value", "jetpackmelon22"));
    final long offset = 0L;
    final SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);

    final List<SinkRecord> sinkRecords = Arrays.asList(sinkRecord);

    // Configure task and write records
    final RedisSinkTask sinkTask = new RedisSinkTask();
    sinkTask.start(config);
    sinkTask.put(sinkRecords);

    StepVerifier
        .create(redisClusterCommands.get("{user.1}.username"))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }
}
