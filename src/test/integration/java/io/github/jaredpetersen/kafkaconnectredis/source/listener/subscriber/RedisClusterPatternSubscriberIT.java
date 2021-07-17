package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class RedisClusterPatternSubscriberIT {
  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_PUB_CONNECTION;
  private static RedisClusterCommands<String, String> REDIS_CLUSTER_PUB_COMMANDS;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_SUB_CONNECTION;

  @BeforeAll
  static void beforeAll() {
    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(REDIS_CLUSTER.getUri());
  }

  @BeforeEach
  void beforeEach() {
    REDIS_CLUSTER_PUB_CONNECTION = REDIS_CLUSTER_CLIENT.connectPubSub();
    REDIS_CLUSTER_PUB_COMMANDS = REDIS_CLUSTER_PUB_CONNECTION.sync();

    REDIS_CLUSTER_SUB_CONNECTION = REDIS_CLUSTER_CLIENT.connectPubSub();
    REDIS_CLUSTER_SUB_CONNECTION.setNodeMessagePropagation(true);
  }

  @AfterEach
  void afterEach() {
    REDIS_CLUSTER_PUB_COMMANDS.flushall();
    REDIS_CLUSTER_PUB_CONNECTION.close();
    REDIS_CLUSTER_SUB_CONNECTION.close();
  }

  @AfterAll
  static void afterAll() {
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  /**
   * Poll the RedisSubscriber until there aren't any messages left to retrieve.
   *
   * @param redisSubscriber RedisSubscriber to poll
   * @return Redis messages retrieved by polling
   */
  static List<RedisMessage> pollUntilEmpty(RedisSubscriber redisSubscriber) {
    final List<RedisMessage> retrievedMessages = new ArrayList<>();

    while (true) {
      final RedisMessage message = redisSubscriber.poll();

      if (message == null) {
        break;
      }

      retrievedMessages.add(message);
    }

    return retrievedMessages;
  }

  @Test
  void pollRetrievesCachedMessagesFromSinglePatternPubSub() throws InterruptedException {
    final String pattern = "*casts";
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(
      REDIS_CLUSTER_SUB_CONNECTION,
      Collections.singletonList(pattern));

    final List<RedisMessage> expectedRedisMessages = IntStream.range(0, 5)
      .mapToObj(i -> RedisMessage.builder()
        .pattern(pattern)
        .channel("podcasts")
        .message(UUID.randomUUID().toString())
        .build())
      .collect(Collectors.toList());

    for (RedisMessage redisMessage : expectedRedisMessages) {
      REDIS_CLUSTER_PUB_COMMANDS.publish(redisMessage.getChannel(), redisMessage.getMessage());
    }

    // Subscription is async and we want to make sure all of the messages are available
    Thread.sleep(2000);

    final List<RedisMessage> retrievedMessages = pollUntilEmpty(redisSubscriber);

    assertEquals(expectedRedisMessages, retrievedMessages);
  }

  @Test
  void pollRetrievesCachedMessagesFromMultiplePatternPubSub() throws InterruptedException {
    final List<String> patterns = Arrays.asList("*casts", "*casters");
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(REDIS_CLUSTER_SUB_CONNECTION, patterns);

    final List<RedisMessage> expectedRedisMessages = IntStream.range(0, 5)
      .mapToObj(i -> {
        final boolean isEven = (i % 2 == 0);
        final String pattern = (isEven) ? "*casts" : "*casters";
        final String channel = (isEven) ? "podcasts" : "podcasters";
        return RedisMessage.builder()
          .pattern(pattern)
          .channel(channel)
          .message(UUID.randomUUID().toString())
          .build();
      })
      .collect(Collectors.toList());

    for (RedisMessage redisMessage : expectedRedisMessages) {
      REDIS_CLUSTER_PUB_COMMANDS.publish(redisMessage.getChannel(), redisMessage.getMessage());
    }

    // Subscription is async and we want to make sure all of the messages are available
    Thread.sleep(2000);

    final List<RedisMessage> retrievedMessages = pollUntilEmpty(redisSubscriber);

    assertEquals(expectedRedisMessages, retrievedMessages);
  }
}
