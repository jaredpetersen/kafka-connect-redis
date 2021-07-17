package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import java.util.concurrent.ConcurrentLinkedQueue;

class RedisClusterListener extends RedisListener implements RedisClusterPubSubListener<String, String>  {
  public RedisClusterListener(ConcurrentLinkedQueue<RedisMessage> messageQueue) {
    super(messageQueue);
  }

  @Override
  public void message(RedisClusterNode node, String channel, String message) {
    message(channel, message);
  }

  @Override
  public void message(RedisClusterNode node, String pattern, String channel, String message) {
    message(pattern, channel, message);
  }

  @Override
  public void subscribed(RedisClusterNode node, String channel, long count) {
    subscribed(channel);
  }

  @Override
  public void psubscribed(RedisClusterNode node, String pattern, long count) {
    psubscribed(pattern);
  }

  @Override
  public void unsubscribed(RedisClusterNode node, String channel, long count) {
    unsubscribed(channel);
  }

  @Override
  public void punsubscribed(RedisClusterNode node, String pattern, long count) {
    punsubscribed(pattern);
  }
}
