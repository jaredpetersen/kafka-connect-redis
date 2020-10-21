package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class WriterIT {
  @Container
  private final GenericContainer redisStandalone = new GenericContainer(DockerImageName.parse("redis:6"))
      .withExposedPorts(6379)
      .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));;

  @Container
  private final GenericContainer redisCluster = new GenericContainer(DockerImageName.parse("redis:6"))
      .withCopyFileToContainer(MountableFile.forClasspathResource("redis/redis-cluster.conf"), "/data/redis.conf")
      .withCopyFileToContainer(MountableFile.forClasspathResource("redis/nodes-cluster.conf"), "/data/nodes.conf")
      .withCommand("redis-server", "/data/redis.conf")
      .withExposedPorts(6379)
      .waitingFor(Wait.forLogMessage(".*Cluster state changed: ok*\\n", 1));

  private RedisClient redisStandaloneClient;
  private RedisReactiveCommands<String, String> redisStandaloneCommands;

  private RedisClusterClient redisClusterClient;
  private RedisClusterReactiveCommands<String, String> redisClusterCommands;

  @BeforeEach
  public void setup() {
    final String redisStandaloneUri = "redis://" + redisStandalone.getHost() + ":" + redisStandalone.getFirstMappedPort();
    this.redisStandaloneClient = RedisClient.create(redisStandaloneUri);
    this.redisStandaloneCommands = redisStandaloneClient.connect().reactive();

    final String redisClusterUri = "redis://" + redisStandalone.getHost() + ":" + redisCluster.getFirstMappedPort();
    this.redisClusterClient = RedisClusterClient.create(redisClusterUri);
    this.redisClusterCommands = redisClusterClient.connect().reactive();
  }

  @AfterEach
  public void cleanup() {
    this.redisStandaloneCommands.flushall().block();
    this.redisStandaloneClient.shutdown();

    this.redisClusterCommands.flushall().block();
    this.redisClusterClient.shutdown();
  }

  @Test
  public void writePartialRedisSetCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .build())
        .build();

    final Writer writer = new Writer(redisStandaloneCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(redisStandaloneCommands.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }

  @Test
  public void writePartialRedisSetCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .build())
        .build();

    final Writer writer = new Writer(redisClusterCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(redisClusterCommands.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }
}
