package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisSubscriptionMessage;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class RedisClusterPatternSubscriberIT {
  @Container
  private static final GenericContainer REDIS_CLUSTER = new GenericContainer(DockerImageName.parse("redis:6"))
    .withCopyFileToContainer(MountableFile.forClasspathResource("redis/redis-cluster.conf"), "/data/redis.conf")
    .withCopyFileToContainer(MountableFile.forClasspathResource("redis/nodes-cluster.conf"), "/data/nodes.conf")
    .withCommand("redis-server", "/data/redis.conf")
    .withExposedPorts(6379)
    .waitingFor(Wait.forLogMessage(".*Cluster state changed: ok*\\n", 1));

  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_PUB_CONNECTION;
  private static RedisClusterReactiveCommands<String, String> REDIS_CLUSTER_PUB_COMMANDS;
  private static StatefulRedisClusterPubSubConnection<String, String> REDIS_CLUSTER_SUB_CONNECTION;

  @BeforeAll
  static void setupAll() {
    final String redisClusterUri = "redis://" + REDIS_CLUSTER.getHost() + ":" + REDIS_CLUSTER.getFirstMappedPort();
    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(redisClusterUri);

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
  public void subscribeSubscribesToPattern() {
    final List<String> patterns = Arrays.asList("*casts", "*casters");
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(REDIS_CLUSTER_SUB_CONNECTION, patterns);

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(0L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(2L)
      .verifyComplete();
  }

  @Test
  public void unsubscribeUnsubscribesFromPatterns() {
    final List<String> patterns = Arrays.asList("*casts", "*casters");
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(REDIS_CLUSTER_SUB_CONNECTION, patterns);

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(2L)
      .verifyComplete();

    StepVerifier
      .create(redisSubscriber.unsubscribe())
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_PUB_CONNECTION.reactive().pubsubNumpat())
      .expectNext(0L)
      .verifyComplete();
  }

  @Test
  public void observeRetrievesPubSubMessages() {
    final String pattern = "*casts";
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(
      REDIS_CLUSTER_SUB_CONNECTION,
      Arrays.asList(pattern));

    final Mono<Void> publish = Flux
      .range(1, 5)
      .flatMapSequential(id -> REDIS_CLUSTER_PUB_COMMANDS.publish("podcasts", "podcast-" + id))
      .then();

    StepVerifier
      .create(redisSubscriber.subscribe())
      .verifyComplete();

    final StepVerifier observeVerifier = StepVerifier
      .create(redisSubscriber.observe())
      .expectNext(RedisSubscriptionMessage.builder().channel("podcasts").pattern(pattern).message("podcast-1").build())
      .expectNext(RedisSubscriptionMessage.builder().channel("podcasts").pattern(pattern).message("podcast-2").build())
      .expectNext(RedisSubscriptionMessage.builder().channel("podcasts").pattern(pattern).message("podcast-3").build())
      .expectNext(RedisSubscriptionMessage.builder().channel("podcasts").pattern(pattern).message("podcast-4").build())
      .expectNext(RedisSubscriptionMessage.builder().channel("podcasts").pattern(pattern).message("podcast-5").build())
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
    final RedisSubscriber redisSubscriber = new RedisClusterPatternSubscriber(REDIS_CLUSTER_SUB_CONNECTION, patterns);

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
