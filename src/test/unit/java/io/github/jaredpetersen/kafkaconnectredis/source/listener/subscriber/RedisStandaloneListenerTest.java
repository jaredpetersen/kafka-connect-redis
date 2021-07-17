package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisStandaloneListenerTest {
  private static final Random random = new Random();

  @Test
  void messageAddsChannelMessageToQueue() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    final String channel = "books";
    final String message = "the best book ever";

    redisStandaloneListener.message(
      channel,
      message);

    final RedisMessage expectedRedisMessage = RedisMessage.builder()
      .channel(channel)
      .message(message)
      .build();

    assertEquals(1, queue.size());
    assertEquals(expectedRedisMessage, queue.poll());
  }

  @Test
  void messageAddsPatternMessageToQueue() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    final String pattern = "b*";
    final String channel = "books";
    final String message = "the best book ever";

    redisStandaloneListener.message(
      pattern,
      channel,
      message);

    final RedisMessage expectedRedisMessage = RedisMessage.builder()
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
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    redisStandaloneListener.subscribed(
      "books",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void psubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    redisStandaloneListener.psubscribed(
      "b*",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void unsubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    redisStandaloneListener.unsubscribed(
      "books",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }

  @Test
  void punsubscribedDoesNothing() {
    final ConcurrentLinkedQueue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
    final RedisStandaloneListener redisStandaloneListener = new RedisStandaloneListener(queue);

    redisStandaloneListener.punsubscribed(
      "b*",
      random.nextInt());

    assertTrue(queue.isEmpty());
  }
}
