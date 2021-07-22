package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis cluster-aware pub/sub subscriber that listens to patterns and caches the retrieved messages for later
 * retrieval.
 */
@Slf4j
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
    subscribePatterns(redisClusterPubSubConnection, patterns);
  }

  /**
   * Subscribe to the provided channels. Re-issue subscriptions asynchronously when the cluster topology changes.
   *
   * @param redisClusterPubSubConnection Cluster pub/sub connection used to facilitate the subscription
   * @param patterns Patterns to subscribe and listen to
   */
  private void subscribePatterns(
    StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
    List<String> patterns
  ) {
    final String[] patternArray = patterns.toArray(new String[0]);

    // Perform an initial subscription
    redisClusterPubSubConnection.sync()
      .upstream()
      .commands()
      .psubscribe(patternArray);

    // Set up a listener to the Lettuce event bus so that we can issue subscriptions to nodes
    redisClusterPubSubConnection.getResources().eventBus().get()
      .filter(event -> event instanceof ClusterTopologyChangedEvent)
      .doOnNext(event -> {
        // Lettuce does its best to determine when the topology changed but there's always a possibility that
        LOG.info("Redis cluster topology changed, issuing new subscriptions");

        redisClusterPubSubConnection.sync()
          .upstream()
          .commands()
          .psubscribe(patternArray);
      })
      .subscribe();
  }
}
