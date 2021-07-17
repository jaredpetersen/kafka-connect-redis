package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Redis cluster-aware pub/sub subscriber that listens to patterns and caches the retrieved messages for later
 * retrieval.
 */
public class RedisClusterPatternSubscriber extends RedisSubscriber {
  /**
   * Create a cluster-aware subscriber that listens to patterns.
   *
   * @param redisClusterPubSubConnection Cluster pub/sub connection used to facilitate the subscription
   * @param patterns Patterns to subscribe and listen to
   */
  public RedisClusterPatternSubscriber(
    StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
    List<String> patterns
  ) {
    super(new ConcurrentLinkedQueue<>());
    redisClusterPubSubConnection.addListener(new RedisClusterListener(this.messageQueue));
    redisClusterPubSubConnection.sync().upstream().commands().psubscribe(patterns.toArray(new String[0]));
  }
}
