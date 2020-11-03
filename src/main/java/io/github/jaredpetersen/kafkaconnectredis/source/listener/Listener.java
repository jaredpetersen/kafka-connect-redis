package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.lettuce.core.cluster.pubsub.api.reactive.RedisClusterPubSubReactiveCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Listen to Redis channels and store the messages in an internal queue for polling.
 */
public class Listener {
  private final RedisPubSubReactiveCommands<String, String> redisStandalonePubSubCommands;
  private final RedisClusterPubSubReactiveCommands<String, String> redisClusterPubSubCommands;
  private final boolean isClusterEnabled;

  private final List<String> channels;
  private final boolean channelsUtilizePatternMatching;

  private final Queue<RedisSubscriptionMessage> queue = new ConcurrentLinkedQueue<>();

  private Disposable listener;

  private static final long MAX_POLL_SIZE = 10_000L;

  private static final Logger LOG = LoggerFactory.getLogger(Listener.class);

  /**
   * Create listener for Redis.
   *
   * @param redisStandalonePubSubCommands Redis standalone commands.
   * @param channels Redis channels to subscribe to.
   * @param channelsUtilizePatternMatching Redis channels utilize pattern matching.
   */
  public Listener(
      RedisPubSubReactiveCommands<String, String> redisStandalonePubSubCommands,
      List<String> channels,
      boolean channelsUtilizePatternMatching) {
    this.redisStandalonePubSubCommands = redisStandalonePubSubCommands;
    this.redisClusterPubSubCommands = null;
    this.isClusterEnabled = false;
    this.channels = channels;
    this.channelsUtilizePatternMatching = channelsUtilizePatternMatching;
  }

  /**
   * Create listener for Redis cluster.
   *
   * @param redisClusterPubSubCommands Redis cluster commands.
   * @param channels Redis channels to subscribe to.
   * @param channelsUtilizePatternMatching Redis channels utilize pattern matching.
   */
  public Listener(
      RedisClusterPubSubReactiveCommands<String, String> redisClusterPubSubCommands,
      List<String> channels,
      boolean channelsUtilizePatternMatching) {
    this.redisStandalonePubSubCommands = null;
    this.redisClusterPubSubCommands = redisClusterPubSubCommands;
    this.isClusterEnabled = true;
    this.channels = channels;
    this.channelsUtilizePatternMatching = channelsUtilizePatternMatching;
  }

  /**
   * Subscribe to channels and begin listening asynchronously.
   */
  public void start() {
    LOG.info("listener start");
    // Subscribe to channels and begin listening
    subscribe();
    listen();
  }

  /**
   * Unsubscribe and stop listening.
   */
  public void stop() {
    if (this.isClusterEnabled) {
      this.redisClusterPubSubCommands.unsubscribe(this.channels.toArray(new String[0])).block();
    }
    else {
      this.redisStandalonePubSubCommands.unsubscribe(this.channels.toArray(new String[0])).block();
    }

    if (this.listener != null) {
      this.listener.dispose();
    }
  }

  /**
   * Poll from the internal queue of records retrieved from the Redis subscription.
   *
   * @return List of Redis messages.
   */
  public List<RedisSubscriptionMessage> poll() {
    final List<RedisSubscriptionMessage> redisMessages = new ArrayList<>();

    while (true) {
      final RedisSubscriptionMessage redisMessage = this.queue.poll();

      // Subscription events may come in faster than we can iterate over them here so return early once we hit the max
      if (redisMessage == null || redisMessages.size() >= MAX_POLL_SIZE) {
        break;
      }

      redisMessages.add(redisMessage);
    }

    return redisMessages;
  }

  private void subscribe() {
    LOG.info("listener subscribe");
    if (this.isClusterEnabled) {
      if (this.channelsUtilizePatternMatching) {
        redisClusterPubSubCommands.psubscribe(channels.toArray(new String[0])).block();
      }
      else {
        redisClusterPubSubCommands.subscribe(channels.toArray(new String[0])).block();
      }
    }
    else {
      if (this.channelsUtilizePatternMatching) {
        redisStandalonePubSubCommands.psubscribe(channels.toArray(new String[0])).block();
      }
      else {
        redisStandalonePubSubCommands.subscribe(channels.toArray(new String[0])).block();
      }
    }
  }

  private void listen() {
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessages;

    if (this.isClusterEnabled) {
      if (this.channelsUtilizePatternMatching) {
        redisSubscriptionMessages = redisClusterPubSubCommands
          .observePatterns()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getChannel())
            .message(channelMessage.getMessage())
            .build());
      }
      else {
        redisSubscriptionMessages = redisClusterPubSubCommands
          .observeChannels()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getChannel())
            .message(channelMessage.getMessage())
            .build());
      }
    }
    else {
      if (this.channelsUtilizePatternMatching) {
        redisSubscriptionMessages = redisStandalonePubSubCommands
          .observePatterns()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getChannel())
            .message(channelMessage.getMessage())
            .build());
      }
      else {
        redisSubscriptionMessages = redisStandalonePubSubCommands
          .observeChannels()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getChannel())
            .message(channelMessage.getMessage())
            .build());
      }
    }

    LOG.info("listener listen");

    this.listener = redisSubscriptionMessages
      .doOnNext(redisMessage -> LOG.info("received message {}", redisMessage))
      .doOnNext(this.queue::add)
      .subscribe();
  }
}
