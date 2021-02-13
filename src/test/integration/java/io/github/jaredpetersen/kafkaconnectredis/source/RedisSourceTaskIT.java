package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class RedisSourceTaskIT {
  @Container
  private static final RedisContainer REDIS_STANDALONE = new RedisContainer();

  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static String REDIS_STANDALONE_URI;
  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static RedisReactiveCommands<String, String> REDIS_STANDALONE_PUB_COMMANDS;
  private static StatefulRedisPubSubConnection<String, String> REDIS_STANDALONE_SUB_CONNECTION;

  private static String REDIS_CLUSTER_URI;
  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static RedisClusterReactiveCommands<String, String> REDIS_CLUSTER_PUB_COMMANDS;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_SUB_CONNECTION;

  @BeforeAll
  static void setupAll() {
    REDIS_STANDALONE_URI = "redis://" + REDIS_STANDALONE.getHost() + ":" + REDIS_STANDALONE.getFirstMappedPort();
    REDIS_STANDALONE_CLIENT = RedisClient.create(REDIS_STANDALONE_URI);

    final StatefulRedisPubSubConnection<String, String> redisStandalonePubConnection = REDIS_STANDALONE_CLIENT
      .connectPubSub();
    REDIS_STANDALONE_PUB_COMMANDS = redisStandalonePubConnection.reactive();

    REDIS_STANDALONE_SUB_CONNECTION = REDIS_STANDALONE_CLIENT.connectPubSub();

    REDIS_CLUSTER_URI = "redis://" + REDIS_CLUSTER.getHost() + ":" + REDIS_CLUSTER.getFirstMappedPort();
    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(REDIS_CLUSTER_URI);

    final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubConnection = REDIS_CLUSTER_CLIENT
      .connectPubSub();
    REDIS_CLUSTER_PUB_COMMANDS = redisClusterPubConnection.reactive();

    REDIS_CLUSTER_SUB_CONNECTION = REDIS_CLUSTER_CLIENT.connectPubSub();
    REDIS_CLUSTER_SUB_CONNECTION.setNodeMessagePropagation(true);
  }

  @AfterEach
  public void cleanupEach() {
    REDIS_STANDALONE_PUB_COMMANDS.flushall().block();
    REDIS_CLUSTER_PUB_COMMANDS.flushall().block();
  }

  @AfterAll
  static void cleanupAll() {
    REDIS_STANDALONE_SUB_CONNECTION.close();
    REDIS_STANDALONE_CLIENT.shutdown();

    REDIS_CLUSTER_SUB_CONNECTION.close();
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  @Test
  public void versionReturnsVersion() {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    assertEquals(VersionUtil.getVersion(), sourceTask.version());
  }

  @Test
  public void pollRetrievesChannelRecordsFromStandalone() throws InterruptedException {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");
    config.put("redis.channels", "boats");
    config.put("redis.channels.pattern.enabled", "false");

    sourceTask.start(config);

    Thread.sleep(1000L);

    final Mono<Void> publish = REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "fishing")
      .then(REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "sport"))
      .then(REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "speed"))
      .then();

    StepVerifier
      .create(publish)
      .verifyComplete();

    Thread.sleep(1000L);

    final List<SourceRecord> sourceRecords = sourceTask.poll();

    assertEquals(3, sourceRecords.size());
  }

  @Test
  public void pollRetrievesPatternRecordsFromStandalone() throws InterruptedException {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");
    config.put("redis.channels", "boat*");
    config.put("redis.channels.pattern.enabled", "true");

    sourceTask.start(config);

    Thread.sleep(1000L);

    final Mono<Void> publish = REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "fishing")
      .then(REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "sport"))
      .then(REDIS_STANDALONE_PUB_COMMANDS.publish("boats", "speed"))
      .then();

    StepVerifier
      .create(publish)
      .verifyComplete();

    Thread.sleep(1000L);

    final List<SourceRecord> sourceRecords = sourceTask.poll();

    assertEquals(3, sourceRecords.size());
  }

  @Test
  public void pollRetrievesChannelRecordsFromCluster() throws InterruptedException {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");
    config.put("redis.channels", "boats");
    config.put("redis.channels.pattern.enabled", "false");

    sourceTask.start(config);

    Thread.sleep(1000L);

    final Mono<Void> publish = REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "fishing")
      .then(REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "sport"))
      .then(REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "speed"))
      .then();

    StepVerifier
      .create(publish)
      .verifyComplete();

    Thread.sleep(1000L);

    final List<SourceRecord> sourceRecords = sourceTask.poll();

    assertEquals(3, sourceRecords.size());
  }

  @Test
  public void pollRetrievesPatternRecordsFromCluster() throws InterruptedException {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");
    config.put("redis.channels", "boat*");
    config.put("redis.channels.pattern.enabled", "true");

    sourceTask.start(config);

    Thread.sleep(1000L);

    final Mono<Void> publish = REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "fishing")
      .then(REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "sport"))
      .then(REDIS_CLUSTER_PUB_COMMANDS.publish("boats", "speed"))
      .then();

    StepVerifier
      .create(publish)
      .verifyComplete();

    Thread.sleep(1000L);

    final List<SourceRecord> sourceRecords = sourceTask.poll();

    assertEquals(3, sourceRecords.size());
  }

  @Test
  public void pollEmptyReturnsEmptyList() {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");
    config.put("redis.channels", "boats");
    config.put("redis.channels.pattern.enabled", "false");

    sourceTask.start(config);

    final List<SourceRecord> sourceRecords = sourceTask.poll();

    assertEquals(0, sourceRecords.size());
  }

  @Test
  public void startThrowsConnectExceptionForInvalidConfig() {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("redis.uri", REDIS_STANDALONE_URI);

    final ConnectException thrown = assertThrows(ConnectException.class, () -> sourceTask.start(taskConfig));
    assertEquals("task configuration error", thrown.getMessage());
  }

  @Test
  public void stopStopsStandalone() {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_STANDALONE_URI);
    config.put("redis.cluster.enabled", "false");
    config.put("redis.channels", "boats");
    config.put("redis.channels.pattern.enabled", "false");

    sourceTask.start(config);
    sourceTask.stop();
  }

  @Test
  public void stopStopsCluster() {
    final RedisSourceTask sourceTask = new RedisSourceTask();

    final Map<String, String> config = new HashMap<>();
    config.put("topic", "mytopic");
    config.put("redis.uri", REDIS_CLUSTER_URI);
    config.put("redis.cluster.enabled", "true");
    config.put("redis.channels", "boat*");
    config.put("redis.channels.pattern.enabled", "true");

    sourceTask.start(config);
    sourceTask.stop();
  }
}
