package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisClusterListenerTest {
  private static final Random random = new Random();

  @Test
  void messageAddsChannelMessageToQueue() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    final String nodeId = UUID.randomUUID().toString();
    final String channel = "books";
    final String message = "the best book ever";

    redisClusterListener.message(
      RedisClusterNode.of(nodeId),
      channel,
      message);

    final RedisMessage expectedRedisMessage = RedisMessage.builder()
      .nodeId(nodeId)
      .channel(channel)
      .message(message)
      .build();

    assertEquals(1, queue.size());
    assertEquals(expectedRedisMessage, queue.poll());
  }

  @Test
  void messageAddsPatternMessageToQueue() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    final String nodeId = UUID.randomUUID().toString();
    final String pattern = "b*";
    final String channel = "books";
    final String message = "the best book ever";

    redisClusterListener.message(
      RedisClusterNode.of(nodeId),
      pattern,
      channel,
      message);

    final RedisMessage expectedRedisMessage = RedisMessage.builder()
      .nodeId(nodeId)
      .pattern(pattern)
      .channel(channel)
      .message(message)
      .build();

    assertEquals(1, queue.size());
    assertEquals(expectedRedisMessage, queue.poll());
  }

  @Test
  void subscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    redisClusterListener.subscribed(
      RedisClusterNode.of(UUID.randomUUID().toString()),
      "books",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void psubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    redisClusterListener.psubscribed(
      RedisClusterNode.of(UUID.randomUUID().toString()),
      "b*",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void unsubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    redisClusterListener.unsubscribed(
      RedisClusterNode.of(UUID.randomUUID().toString()),
      "books",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void punsubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisClusterListener redisClusterListener = new RedisClusterListener(queue);

    redisClusterListener.punsubscribed(
      RedisClusterNode.of(UUID.randomUUID().toString()),
      "b*",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }
}
