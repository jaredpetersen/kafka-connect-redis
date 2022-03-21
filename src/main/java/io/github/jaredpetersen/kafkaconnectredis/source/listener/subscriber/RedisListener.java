package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class RedisListener {
  protected final ConcurrentLinkedQueue<RedisMessage> messageQueue;

  public RedisListener(ConcurrentLinkedQueue<RedisMessage> messageQueue) {
    this.messageQueue = messageQueue;
  }

  public void message(String channel, String message) {
    final RedisMessage redisMessage = RedisMessage.builder()
      .channel(channel)
      .message(message)
      .build();

    messageQueue.add(redisMessage);
  }

  public void message(String pattern, String channel, String message) {
    final RedisMessage redisMessage = RedisMessage.builder()
      .pattern(pattern)
      .channel(channel)
      .message(message)
      .build();

    messageQueue.add(redisMessage);
  }

  public void subscribed(String channel) {
    LOG.info("Subscribed to channel {}", channel);
  }

  public void psubscribed(String pattern) {
    LOG.info("Subscribed to pattern {}", pattern);
  }

  public void unsubscribed(String channel) {
    LOG.info("Unsubscribed from channel {}", channel);
  }

  public void punsubscribed(String pattern) {
    LOG.info("Unsubscribed from pattern {}", pattern);
  }
}
