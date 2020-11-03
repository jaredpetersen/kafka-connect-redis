package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.reactive.RedisClusterPubSubReactiveCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class RedisClusterListener implements RedisListener {
  private final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;
  private final RedisClusterPubSubReactiveCommands<String, String> redisClusterPubSubCommands;

  private final List<String> channels;
  private final boolean channelsUtilizePatternMatching;

  private final Queue<RedisSubscriptionMessage> queue = new ConcurrentLinkedQueue<>();

  private Disposable listen;

  private static final long MAX_POLL_SIZE = 10_000L;

  private static final Logger LOG = LoggerFactory.getLogger(RedisClusterListener.class);

  public RedisClusterListener(
      StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
      List<String> channels,
      boolean channelsUtilizePatternMatching) {
    this.redisClusterPubSubConnection = redisClusterPubSubConnection;
    this.redisClusterPubSubCommands = this.redisClusterPubSubConnection.reactive();
    this.channels = channels;
    this.channelsUtilizePatternMatching = channelsUtilizePatternMatching;
  }

  @Override
  public void start() {
    // Subscribe to channels
    if (channelsUtilizePatternMatching) {
      redisClusterPubSubCommands.psubscribe(channels.toArray(new String[0])).block();
    }
    else {
      redisClusterPubSubCommands.subscribe(channels.toArray(new String[0])).block();
    }

    // Set up subscription client and message converter
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessages = (this.channelsUtilizePatternMatching)
      ? redisClusterPubSubCommands
          .observePatterns()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getMessage())
            .message(channelMessage.getMessage())
            .build())
      : redisClusterPubSubCommands
          .observeChannels()
          .map(channelMessage -> RedisSubscriptionMessage.builder()
            .channel(channelMessage.getMessage())
            .message(channelMessage.getMessage())
            .build());

    // Begin listening
    this.listen = redisSubscriptionMessages
      .doOnNext(redisMessage -> LOG.info("received message {}", redisMessage))
      .doOnNext(this.queue::add)
      .subscribe();
  }

  @Override
  public void stop() {
    if (this.listen != null) {
      this.listen.dispose();
    }
  }

  @Override
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
}
