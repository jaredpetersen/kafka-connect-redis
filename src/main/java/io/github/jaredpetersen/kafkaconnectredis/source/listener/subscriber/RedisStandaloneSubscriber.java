package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.pubsub.RedisPubSubListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis subscriber for standalone instances that stores events internally for retrieval.
 *
 * @param <K> Channel / pattern
 * @param <V> Message
 */
public class RedisStandaloneSubscriber<K, V> extends RedisPollingSubscriber<K, V> implements RedisPubSubListener<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(RedisStandaloneSubscriber.class);

  @Override
  public void message(K channel, V message) {
    LOG.info("message: {} {}", channel, message);
    this.recordEvent(channel, message);
  }

  @Override
  public void message(K pattern, K channel, V message) {
    LOG.info("pmessage: {} {} {}", pattern, channel, message);
    this.recordEvent(channel, message);
  }

  @Override
  public void subscribed(K channel, long count) {
    LOG.info("subscribed: {} {}", channel, count);
  }

  @Override
  public void psubscribed(K pattern, long count) {
    LOG.info("psubscribed: {} {}", pattern, count);
  }

  @Override
  public void unsubscribed(K channel, long count) {
    LOG.info("unsubscribed: {} {}", channel, count);
  }

  @Override
  public void punsubscribed(K pattern, long count) {
    LOG.info("punsubscribed: {} {}", pattern, count);
  }
}
