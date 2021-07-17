package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
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
class RedisChannelSubscriberIT {
  @Container
  private static final RedisContainer REDIS_STANDALONE = new RedisContainer();

  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static StatefulRedisPubSubConnection<String, String> REDIS_STANDALONE_PUB_CONNECTION;
  private static RedisCommands<String, String> REDIS_STANDALONE_PUB_COMMANDS;
  private static StatefulRedisPubSubConnection<String, String> REDIS_STANDALONE_SUB_CONNECTION;

  @BeforeAll
  static void beforeAll() {
    REDIS_STANDALONE_CLIENT = RedisClient.create(REDIS_STANDALONE.getUri());
  }

  @BeforeEach
  void beforeEach() {
    REDIS_STANDALONE_PUB_CONNECTION = REDIS_STANDALONE_CLIENT.connectPubSub();
    REDIS_STANDALONE_PUB_COMMANDS = REDIS_STANDALONE_PUB_CONNECTION.sync();

    REDIS_STANDALONE_SUB_CONNECTION = REDIS_STANDALONE_CLIENT.connectPubSub();
  }

  @AfterEach
  void afterEach() {
    REDIS_STANDALONE_PUB_COMMANDS.flushall();
    REDIS_STANDALONE_PUB_CONNECTION.close();
    REDIS_STANDALONE_SUB_CONNECTION.close();
  }

  @AfterAll
  static void afterAll() {
    REDIS_STANDALONE_CLIENT.shutdown();
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
  void pollRetrievesCachedMessagesFromSingleChannelPubSub() throws InterruptedException {
    final String channel = "podcasts";
    final RedisSubscriber redisSubscriber = new RedisChannelSubscriber(
      REDIS_STANDALONE_SUB_CONNECTION,
      Collections.singletonList(channel));

    final List<RedisMessage> expectedRedisMessages = IntStream.range(0, 5)
      .mapToObj(i -> RedisMessage.builder()
        .channel(channel)
        .message(UUID.randomUUID().toString())
        .build())
      .collect(Collectors.toList());

    for (RedisMessage redisMessage : expectedRedisMessages) {
      REDIS_STANDALONE_PUB_COMMANDS.publish(redisMessage.getChannel(), redisMessage.getMessage());
    }

    // Subscription is async and we want to make sure all of the messages are available
    Thread.sleep(2000);

    final List<RedisMessage> retrievedMessages = pollUntilEmpty(redisSubscriber);

    assertEquals(expectedRedisMessages, retrievedMessages);
  }

  @Test
  void pollRetrievesCachedMessagesFromMultipleChannelPubSub() throws InterruptedException {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisChannelSubscriber(REDIS_STANDALONE_SUB_CONNECTION, channels);

    final List<RedisMessage> expectedRedisMessages = IntStream.range(0, 5)
      .mapToObj(i -> {
        final String channel = (i % 2 == 0) ? "podcasts" : "podcasters";
        return RedisMessage.builder()
          .channel(channel)
          .message(UUID.randomUUID().toString())
          .build();
      })
      .collect(Collectors.toList());

    for (RedisMessage redisMessage : expectedRedisMessages) {
      REDIS_STANDALONE_PUB_COMMANDS.publish(redisMessage.getChannel(), redisMessage.getMessage());
    }

    // Subscription is async and we want to make sure all of the messages are available
    Thread.sleep(2000);

    final List<RedisMessage> retrievedMessages = pollUntilEmpty(redisSubscriber);

    assertEquals(expectedRedisMessages, retrievedMessages);
  }
}
