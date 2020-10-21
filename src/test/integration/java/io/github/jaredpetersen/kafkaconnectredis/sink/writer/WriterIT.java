package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class WriterIT {
  @Container
  private final GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:6")).withExposedPorts(6379);

  private RedisClient redisStandaloneClient;
  private RedisReactiveCommands<String, String> redisStandaloneCommands;

  @BeforeEach
  public void setup() {
    final String redisUri = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    this.redisStandaloneClient = RedisClient.create(redisUri);
    this.redisStandaloneCommands = redisStandaloneClient.connect().reactive();
  }

  @AfterEach
  public void cleanup() {
    this.redisStandaloneCommands.flushall().block();
    this.redisStandaloneClient.shutdown();
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
}
