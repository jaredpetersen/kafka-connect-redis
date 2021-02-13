package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class RedisPatternSubscriberIT {
  @Container
  private static final GenericContainer<?> REDIS_STANDALONE = new GenericContainer<>(DockerImageName.parse("redis:6"))
    .withExposedPorts(6379)
    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static StatefulRedisPubSubConnection<String, String> REDIS_STANDALONE_PUB_CONNECTION;
  private static RedisReactiveCommands<String, String> REDIS_STANDALONE_PUB_COMMANDS;
  private static StatefulRedisPubSubConnection<String, String> REDIS_STANDALONE_SUB_CONNECTION;

  @BeforeAll
  static void setupAll() {
    final String redisClusterUri = "redis://"
      + REDIS_STANDALONE.getHost()
      + ":"
      + REDIS_STANDALONE.getFirstMappedPort();
    REDIS_STANDALONE_CLIENT = RedisClient.create(redisClusterUri);

    REDIS_STANDALONE_PUB_CONNECTION = REDIS_STANDALONE_CLIENT.connectPubSub();
    REDIS_STANDALONE_PUB_COMMANDS = REDIS_STANDALONE_PUB_CONNECTION.reactive();

    REDIS_STANDALONE_SUB_CONNECTION = REDIS_STANDALONE_CLIENT.connectPubSub();
  }

  @AfterEach
  public void cleanupEach() {
    REDIS_STANDALONE_PUB_COMMANDS.flushall().block();
  }

  @AfterAll
  static void cleanupAll() {
    REDIS_STANDALONE_SUB_CONNECTION.close();
    REDIS_STANDALONE_CLIENT.shutdown();
  }

  @Test
  public void subscribeSubscribesToPattern() {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisPatternSubscriber(REDIS_STANDALONE_SUB_CONNECTION, channels);

    StepVerifier
      .create(REDIS_STANDALONE_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(0L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(2L)
      .verifyComplete();
  }

  @Test
  public void unsubscribeUnsubscribesFromPatterns() {
    final List<String> channels = Arrays.asList("podcasts", "podcasters");
    final RedisSubscriber redisSubscriber = new RedisPatternSubscriber(REDIS_STANDALONE_SUB_CONNECTION, channels);

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(2L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.unsubscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(0L)
      .verifyComplete();
  }

  @Test
  public void observeRetrievesPubSubMessages() {
    final String pattern = "*casts";
    final RedisSubscriber redisSubscriber = new RedisPatternSubscriber(
      REDIS_STANDALONE_SUB_CONNECTION,
      Arrays.asList(pattern));

    final Mono<Void> publish = Flux
      .range(1, 5)
      .flatMapSequential(id -> REDIS_STANDALONE_PUB_COMMANDS.publish("podcasts", "podcast-" + id))
      .then();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    final StepVerifier observeVerifier = StepVerifier
      .create(redisSubscriber.observe())
      .expectNext(RedisMessage.builder().channel("podcasts").pattern(pattern).message("podcast-1").build())
      .expectNext(RedisMessage.builder().channel("podcasts").pattern(pattern).message("podcast-2").build())
      .expectNext(RedisMessage.builder().channel("podcasts").pattern(pattern).message("podcast-3").build())
      .expectNext(RedisMessage.builder().channel("podcasts").pattern(pattern).message("podcast-4").build())
      .expectNext(RedisMessage.builder().channel("podcasts").pattern(pattern).message("podcast-5").build())
      .expectNoEvent(Duration.ofSeconds(2L))
      .thenCancel()
      .verifyLater();

    StepVerifier
      .create(publish)
      .verifyComplete();

    observeVerifier.verify();
  }

  @Test
  public void observeRetrievesPubSubMessagesFromMultiplePattern() {
    final List<String> patterns = Arrays.asList("*casts", "*casters");
    final RedisSubscriber redisSubscriber = new RedisPatternSubscriber(REDIS_STANDALONE_SUB_CONNECTION, patterns);

    final Mono<Void> publish = Flux
      .range(1, 5)
      .flatMapSequential(id -> REDIS_STANDALONE_PUB_COMMANDS.publish("podcasts", "podcast-" + id)
        .then(REDIS_STANDALONE_PUB_COMMANDS.publish("podcasters", "podcaster-" + id)))
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
