package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Redis pub/sub subscriber that listens to patterns and caches the retrieved messages for later retrieval.
 */
public class RedisPatternSubscriber extends RedisSubscriber {
  /**
   * Create a subscriber that listens to patterns.
   *
   * @param redisPubSubConnection Pub/sub connection used to facilitate the subscription
   * @param patterns Patterns to subscribe and listen to
   */
  public RedisPatternSubscriber(
    StatefulRedisPubSubConnection<String, String> redisPubSubConnection,
    List<String> patterns
  ) {
    super(new ConcurrentLinkedQueue<>());
    redisPubSubConnection.addListener(new RedisStandaloneListener(this.messageQueue));
    redisPubSubConnection.sync().psubscribe(patterns.toArray(new String[0]));
  }
}
