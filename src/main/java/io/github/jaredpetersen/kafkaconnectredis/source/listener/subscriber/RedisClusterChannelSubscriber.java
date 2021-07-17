package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Redis cluster-aware pub/sub subscriber that listens to channels and caches the retrieved messages for later
 * retrieval.
 */
public class RedisClusterChannelSubscriber extends RedisSubscriber {
  /**
   * Create a cluster-aware subscriber that listens to channels.
   *
   * @param redisClusterPubSubConnection Cluster pub/sub connection used to facilitate the subscription
   * @param channels Channels to subscribe and listen to
   */
  public RedisClusterChannelSubscriber(
    StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
    List<String> channels
  ) {
    super(new ConcurrentLinkedQueue<>());
    redisClusterPubSubConnection.addListener(new RedisClusterListener(this.messageQueue));
    redisClusterPubSubConnection.sync().upstream().commands().subscribe(channels.toArray(new String[0]));
  }
}
