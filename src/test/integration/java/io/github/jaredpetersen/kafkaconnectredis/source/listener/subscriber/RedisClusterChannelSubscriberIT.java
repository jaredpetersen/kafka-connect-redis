package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class RedisClusterChannelSubscriberIT {
  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_PUB_CONNECTION;
  private static RedisClusterReactiveCommands<String, String> REDIS_CLUSTER_PUB_COMMANDS;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_SUB_CONNECTION;

  @BeforeAll
  static void setupAll() {
    final String redisStandaloneUri = "redis://" + REDIS_CLUSTER.getHost() + ":" + REDIS_CLUSTER.getFirstMappedPort();
    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(redisStandaloneUri);

    REDIS_CLUSTER_PUB_CONNECTION = REDIS_CLUSTER_CLIENT.connectPubSub();
    REDIS_CLUSTER_PUB_COMMANDS = REDIS_CLUSTER_PUB_CONNECTION.reactive();

    REDIS_CLUSTER_SUB_CONNECTION = REDIS_CLUSTER_CLIENT.connectPubSub();
    REDIS_CLUSTER_SUB_CONNECTION.setNodeMessagePropagation(true);
  }

  @AfterEach
  public void cleanupEach() {
    REDIS_CLUSTER_PUB_COMMANDS.flushall().block();
  }

  @AfterAll
  static void cleanupAll() {
    REDIS_CLUSTER_SUB_CONNECTION.close();
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  @Test
  public void subscribeSubscribesToChannels() {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisClusterChannelSubscriber(REDIS_CLUSTER_SUB_CONNECTION, channels);

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumsub(channels.toArray(new String[0])))
      .expectNextMatches(channelMap ->
        channelMap.get(channels.get(0)) == 0L && channelMap.get(channels.get(1)) == 0L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumsub(channels.toArray(new String[0])))
      .expectNextMatches(channelMap ->
        channelMap.get(channels.get(0)) == 1L && channelMap.get(channels.get(1)) == 1L)
      .verifyComplete();
  }

  @Test
  public void unsubscribeUnsubscribesFromChannels() {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisClusterChannelSubscriber(REDIS_CLUSTER_SUB_CONNECTION, channels);

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumsub(channels.toArray(new String[0])))
      .expectNextMatches(channelMap ->
        channelMap.get(channels.get(0)) == 1L && channelMap.get(channels.get(1)) == 1L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.unsubscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumsub(channels.toArray(new String[0])))
      .expectNextMatches(channelMap ->
        channelMap.get(channels.get(0)) == 0 && channelMap.get(channels.get(1)) == 0)
      .verifyComplete();
  }

  @Test
  public void observeRetrievesPubSubMessages() {
    final String channel = "podcasts";
    final RedisSubscriber redisSubscriber = new RedisClusterChannelSubscriber(
      REDIS_CLUSTER_SUB_CONNECTION,
      Arrays.asList(channel));

    final Mono<Void> publish = Flux
      .range(1, 5)
      .flatMapSequential(id -> REDIS_CLUSTER_PUB_COMMANDS.publish("podcasts", "podcast-" + id))
      .then();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    final StepVerifier observeVerifier = StepVerifier
      .create(redisSubscriber.observe())
      .expectNext(RedisMessage.builder().channel(channel).message("podcast-1").build())
      .expectNext(RedisMessage.builder().channel(channel).message("podcast-2").build())
      .expectNext(RedisMessage.builder().channel(channel).message("podcast-3").build())
      .expectNext(RedisMessage.builder().channel(channel).message("podcast-4").build())
      .expectNext(RedisMessage.builder().channel(channel).message("podcast-5").build())
      .expectNoEvent(Duration.ofSeconds(2L))
      .thenCancel()
      .verifyLater();

    StepVerifier
      .create(publish)
      .verifyComplete();

    observeVerifier.verify();
  }

  @Test
  public void observeRetrievesPubSubMessagesFromMultipleChannels() {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisClusterChannelSubscriber(REDIS_CLUSTER_SUB_CONNECTION, channels);

    final Mono<Void> publish = Flux
      .range(1, 5)
      .flatMapSequential(id -> REDIS_CLUSTER_PUB_COMMANDS.publish("podcasts", "podcast-" + id)
        .then(REDIS_CLUSTER_PUB_COMMANDS.publish("podcasters", "podcaster-" + id)))
      .then();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    final StepVerifier observeVerifier = StepVerifier
      .create(redisSubscriber.observe())
      .expectNextCount(10)
      .expectNoEvent(Duration.ofSeconds(2L))
      .thenCancel()
      .verifyLater();

    StepVerifier
      .create(publish)
      .verifyComplete();

    observeVerifier.verify();
  }
}
