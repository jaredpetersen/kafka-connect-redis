package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis subscriber for clusters that stores events internally for retrieval.
 *
 * @param <K> Channel / pattern
 * @param <V> Message
 */
public class RedisClusterSubscriber<K, V>
    extends RedisPollingSubscriber<K, V> implements RedisClusterPubSubListener<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(RedisClusterSubscriber.class);

  @Override
  public void message(RedisClusterNode redisClusterNode, K channel, V message) {
    LOG.info("message: {} {} {}", redisClusterNode, channel, message);
    this.recordEvent(channel, message);
  }

  @Override
  public void message(RedisClusterNode redisClusterNode, K channel, K pattern, V message) {
    LOG.info("pmessage: {} {} {} {}", redisClusterNode, channel, pattern, message);
    this.recordEvent(channel, message);
  }

  @Override
  public void subscribed(RedisClusterNode redisClusterNode, K channel, long count) {
    LOG.info("subscribed: {} {} {}", redisClusterNode, channel, count);
  }

  @Override
  public void psubscribed(RedisClusterNode redisClusterNode, K pattern, long count) {
    LOG.info("psubscribed: {} {} {}", redisClusterNode, pattern, count);
  }

  @Override
  public void unsubscribed(RedisClusterNode redisClusterNode, K channel, long count) {
    LOG.info("unsubscribed: {} {} {}", redisClusterNode, channel, count);
  }

  @Override
  public void punsubscribed(RedisClusterNode redisClusterNode, Object o, long l) {
    LOG.info("pusubscribed: {} {} {}", redisClusterNode, o, l);
  }
}
