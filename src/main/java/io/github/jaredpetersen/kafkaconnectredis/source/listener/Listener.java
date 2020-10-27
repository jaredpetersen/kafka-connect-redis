package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisClusterSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisStandaloneSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.SubscriptionEvent;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.reactive.RedisClusterPubSubReactiveCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import java.util.List;

public class Listener {
  private final StatefulRedisPubSubConnection<String, String> redisStandalonePubSubConnection;
  private final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;
  private final boolean clusterEnabled;

  private final List<String> channels;
  private final boolean channelsUtilizePatternMatching;

  private final RedisClusterSubscriber<String, String> redisClusterSubscriber = new RedisClusterSubscriber<>();
  private final RedisStandaloneSubscriber<String, String> redisSubscriber = new RedisStandaloneSubscriber<>();

  /**
   * Set up listener to interact with standalone Redis and begin listening.
   *
   * @param redisStandalonePubSubConnection Standalone Redis to listen to.
   * @param channels Channels to subscribe to.
   * @param channelsUtilizePatternMatching Channels utilize pattern matching.
   */
  public Listener(
      StatefulRedisPubSubConnection<String, String> redisStandalonePubSubConnection,
      List<String> channels,
      boolean channelsUtilizePatternMatching) {
    this.redisStandalonePubSubConnection = redisStandalonePubSubConnection;
    this.redisClusterPubSubConnection = null;
    this.clusterEnabled = false;
    this.channels = channels;
    this.channelsUtilizePatternMatching = channelsUtilizePatternMatching;

    this.start();
  }

  /**
   * Set up listener to interact with Redis cluster and begin listening.
   *
   * @param redisClusterPubSubConnection Redis cluster to listen to.
   * @param channels Channels to subscribe to.
   * @param channelsUtilizePatternMatching Channels utilize pattern matching.
   */
  public Listener(
      StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
      List<String> channels,
      boolean channelsUtilizePatternMatching) {
    this.redisStandalonePubSubConnection = null;
    this.redisClusterPubSubConnection = redisClusterPubSubConnection;
    this.clusterEnabled = true;
    this.channels = channels;
    this.channelsUtilizePatternMatching = channelsUtilizePatternMatching;

    this.start();
  }

  /**
   * Retrieve all of the recently listened to items from Redis in the order that they arrived.
   *
   * @return Recently listened to items from Redis.
   */
  public List<SubscriptionEvent<String, String>> poll() {
    return (this.clusterEnabled)
      ? this.redisClusterSubscriber.poll()
      : this.redisSubscriber.poll();
  }

  private void start() {
    if (this.clusterEnabled) {
      this.redisClusterPubSubConnection.addListener(redisClusterSubscriber);

      final RedisClusterPubSubReactiveCommands<String, String> redisClusterPubSubCommands =
        this.redisClusterPubSubConnection.reactive();

      if (this.channelsUtilizePatternMatching) {
        redisClusterPubSubCommands.upstream().commands().psubscribe(channels.toArray(new String[0]));
      }
      else {
        redisClusterPubSubCommands.upstream().commands().subscribe(channels.toArray(new String[0]));
      }
    }
    else {
      this.redisStandalonePubSubConnection.addListener(redisSubscriber);

      final RedisPubSubReactiveCommands<String, String> redisPubSubCommands = this.redisStandalonePubSubConnection
        .reactive();

      if (this.channelsUtilizePatternMatching) {
        redisPubSubCommands.psubscribe(channels.toArray(new String[0]));
      }
      else {
        redisPubSubCommands.subscribe(channels.toArray(new String[0]));
      }
    }
  }
}
