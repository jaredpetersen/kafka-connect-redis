package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis cluster-aware pub/sub subscriber that listens to channels and caches the retrieved messages for later
 * retrieval.
 */
@Slf4j
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
    subscribeChannels(redisClusterPubSubConnection, channels);
  }

  /**
   * Subscribe to the provided channels. Re-issue subscriptions asynchronously when the cluster topology changes.
   *
   * @param redisClusterPubSubConnection Cluster pub/sub connection used to facilitate the subscription
   * @param channels Channels to subscribe and listen to
   */
  private void subscribeChannels(
    StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
    List<String> channels
  ) {
    final String[] channelArray = channels.toArray(new String[0]);

    // Perform an initial subscription
    redisClusterPubSubConnection.sync()
      .upstream()
      .commands()
      .subscribe(channelArray);

    // Set up a listener to the Lettuce event bus so that we can issue subscriptions to nodes
    redisClusterPubSubConnection.getResources().eventBus().get()
      .filter(event -> event instanceof ClusterTopologyChangedEvent)
      .doOnNext(event -> {
        // Lettuce does its best to determine when the topology changed but there's always a possibility that
        LOG.info("Redis cluster topology changed, issuing new subscriptions");

        redisClusterPubSubConnection.sync()
          .upstream()
          .commands()
          .subscribe(channelArray);
      })
      .subscribe();
  }
}
