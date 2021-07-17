package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.pubsub.RedisPubSubListener;
import java.util.concurrent.ConcurrentLinkedQueue;

class RedisStandaloneListener extends RedisListener implements RedisPubSubListener<String, String> {
  public RedisStandaloneListener(ConcurrentLinkedQueue<RedisMessage> messageQueue) {
    super(messageQueue);
  }

  @Override
  public void subscribed(String channel, long count) {
    subscribed(channel);
  }

  @Override
  public void psubscribed(String pattern, long count) {
    psubscribed(pattern);
  }

  @Override
  public void unsubscribed(String channel, long count) {
    unsubscribed(channel);
  }

  @Override
  public void punsubscribed(String pattern, long count) {
    punsubscribed(pattern);
  }
}
