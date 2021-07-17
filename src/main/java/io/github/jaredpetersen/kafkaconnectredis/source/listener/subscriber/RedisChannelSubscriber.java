package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Redis pub/sub subscriber that listens to channels and caches the retrieved messages for later retrieval.
 */
public class RedisChannelSubscriber extends RedisSubscriber {
  /**
   * Create a subscriber that listens to channels.
   *
   * @param redisPubSubConnection Pub/sub connection used to facilitate the subscription
   * @param channels Patterns to subscribe and listen to
   */
  public RedisChannelSubscriber(
    StatefulRedisPubSubConnection<String, String> redisPubSubConnection,
    List<String> channels
  ) {
    super(new ConcurrentLinkedQueue<>());
    redisPubSubConnection.addListener(new RedisStandaloneListener(this.messageQueue));
    redisPubSubConnection.sync().subscribe(channels.toArray(new String[0]));
  }
}
