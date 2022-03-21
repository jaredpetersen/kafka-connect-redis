package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RedisClusterListener extends RedisListener implements RedisClusterPubSubListener<String, String>  {
  public RedisClusterListener(ConcurrentLinkedQueue<RedisMessage> messageQueue) {
    super(messageQueue);
  }

  @Override
  public void message(RedisClusterNode node, String channel, String message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received channel {} from node {}", channel, node.getNodeId());
    }
    final RedisMessage redisMessage = RedisMessage.builder()
      .nodeId(node.getNodeId())
      .channel(channel)
      .message(message)
      .build();

    messageQueue.add(redisMessage);
  }

  public void message(RedisClusterNode node, String pattern, String channel, String message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received channel {} from node {}", channel, node.getNodeId());
    }
    final RedisMessage redisMessage = RedisMessage.builder()
      .nodeId(node.getNodeId())
      .pattern(pattern)
      .channel(channel)
      .message(message)
      .build();

    messageQueue.add(redisMessage);
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
