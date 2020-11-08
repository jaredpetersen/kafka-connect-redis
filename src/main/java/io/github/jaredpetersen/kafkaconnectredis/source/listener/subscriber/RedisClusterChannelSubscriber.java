package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisClusterChannelSubscriber implements RedisSubscriber {
  private final StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;
  private final List<String> channels;

  public RedisClusterChannelSubscriber(
      StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection,
      List<String> channels) {
    this.redisClusterPubSubConnection = redisClusterPubSubConnection;
    this.channels = channels;
  }

  @Override
  public Mono<Void> subscribe() {
    return redisClusterPubSubConnection.reactive()
      .upstream()
      .commands()
      .subscribe(channels.toArray(new String[0]))
      .flux()
      .then();
  }

  @Override
  public Mono<Void> unsubscribe() {
    return redisClusterPubSubConnection.reactive()
      .upstream()
      .commands()
      .unsubscribe(channels.toArray(new String[0]))
      .flux()
      .then();
  }

  @Override
  public Flux<RedisMessage> observe() {
    return redisClusterPubSubConnection.reactive().observeChannels()
      .map(channelMessage -> RedisMessage.builder()
        .channel(channelMessage.getChannel())
        .message(channelMessage.getMessage())
        .build());
  }
}
