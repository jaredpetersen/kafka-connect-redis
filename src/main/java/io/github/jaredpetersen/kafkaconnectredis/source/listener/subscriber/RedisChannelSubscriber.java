package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RedisChannelSubscriber implements RedisSubscriber {
  private final StatefulRedisPubSubConnection<String, String> redisPubSubConnection;
  private final List<String> channels;

  public RedisChannelSubscriber(
      StatefulRedisPubSubConnection<String, String> redisPubSubConnection,
      List<String> channels) {
    this.redisPubSubConnection = redisPubSubConnection;
    this.channels = channels;
  }

  @Override
  public Mono<Void> subscribe() {
    return redisPubSubConnection.reactive()
      .subscribe(channels.toArray(new String[0]));
  }

  @Override
  public Mono<Void> unsubscribe() {
    return redisPubSubConnection.reactive()
      .unsubscribe(channels.toArray(new String[0]));
  }

  @Override
  public Flux<RedisMessage> observe() {
    return redisPubSubConnection.reactive().observeChannels()
      .map(channelMessage -> RedisMessage.builder()
        .channel(channelMessage.getChannel())
        .message(channelMessage.getMessage())
        .build());
  }
}
