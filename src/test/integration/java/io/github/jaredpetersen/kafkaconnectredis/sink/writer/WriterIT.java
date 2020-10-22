package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
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

import java.util.Arrays;

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

  @Test
  public void writeRedisSetCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
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

    StepVerifier
        .create(redisStandaloneCommands.ttl(redisCommand.getPayload().getKey()))
        .expectNext(redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeRedisSetCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
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

  @Test
  public void writeRedisSaddCommandAppliesCommandToStandalone() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList("fishing", "sport", "tug"))
            .build())
        .build();

    final Writer writer = new Writer(redisStandaloneCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(redisStandaloneCommands.smembers(redisCommand.getPayload().getKey()))
        .thenConsumeWhile(member -> redisCommand.getPayload().getValues().contains(member))
        .verifyComplete();
  }

  @Test
  public void writeRedisSaddCommandAppliesCommandToCluster() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList("fishing", "sport", "tug"))
            .build())
        .build();

    final Writer writer = new Writer(redisClusterCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(redisClusterCommands.smembers(redisCommand.getPayload().getKey()))
        .thenConsumeWhile(member -> redisCommand.getPayload().getValues().contains(member))
        .verifyComplete();
  }

  @Test
  public void writeRedisGeoaddCommandAppliesCommandToStandalone() {
    final RedisGeoaddCommand redisCommand = RedisGeoaddCommand.builder()
        .payload(RedisGeoaddCommand.Payload.builder()
            .key("Sicily")
            .values(Arrays.asList(
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(13.361389d)
                    .latitude(38.115556d)
                    .member("Palermo")
                    .build(),
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(15.087269d)
                    .latitude(37.502669d)
                    .member("Catania")
                    .build()
            ))
            .build())
        .build();

    final Writer writer = new Writer(redisStandaloneCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    // Beware, Redis does not store exact coordinates

    StepVerifier
        .create(redisStandaloneCommands.geopos(
            redisCommand.getPayload().getKey(),
            redisCommand.getPayload().getValues().get(0).getMember(),
            redisCommand.getPayload().getValues().get(1).getMember()))
        .thenConsumeWhile(value -> {
          final double actualLongitude = value.getValue().getX().doubleValue();
          final double actualLatitude = value.getValue().getY().doubleValue();

          // Beware, Redis does not store exact coordinates
          return (actualLongitude == 13.36138933897018433d && actualLatitude == 38.11555639549629859d)
              || (actualLongitude == 15.087267458438873 && actualLatitude == 37.50266842333162);
        })
        .verifyComplete();
  }

  @Test
  public void writeRedisGeoaddCommandAppliesCommandToCluster() {
    final RedisGeoaddCommand redisCommand = RedisGeoaddCommand.builder()
        .payload(RedisGeoaddCommand.Payload.builder()
            .key("Sicily")
            .values(Arrays.asList(
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(13.361389d)
                    .latitude(38.115556d)
                    .member("Palermo")
                    .build(),
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(15.087269d)
                    .latitude(37.502669d)
                    .member("Catania")
                    .build()
            ))
            .build())
        .build();

    final Writer writer = new Writer(redisClusterCommands);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(redisClusterCommands.geopos(
            redisCommand.getPayload().getKey(),
            redisCommand.getPayload().getValues().get(0).getMember(),
            redisCommand.getPayload().getValues().get(1).getMember()))
        .thenConsumeWhile(value -> {
          final double actualLongitude = value.getValue().getX().doubleValue();
          final double actualLatitude = value.getValue().getY().doubleValue();

          // Beware, Redis does not store exact coordinates
          return (actualLongitude == 13.36138933897018433d && actualLatitude == 38.11555639549629859d)
              || (actualLongitude == 15.087267458438873 && actualLatitude == 37.50266842333162);
        })
        .verifyComplete();
  }
}
