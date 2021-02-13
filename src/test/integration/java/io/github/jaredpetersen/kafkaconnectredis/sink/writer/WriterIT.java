package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisArbitraryCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireatCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisPexpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import java.time.Instant;
import java.util.Arrays;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
public class WriterIT {
  @Container
  private static final RedisContainer REDIS_STANDALONE = new RedisContainer();

  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static StatefulRedisConnection<String, String> REDIS_STANDALONE_CONNECTION;
  private static RedisReactiveCommands<String, String> REDIS_STANDALONE_COMMANDS;

  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterConnection<String, String> REDIS_CLUSTER_CONNECTION;
  private static RedisClusterReactiveCommands<String, String> REDIS_CLUSTER_COMMANDS;

  @BeforeAll
  static void setupAll() {
    REDIS_STANDALONE_CLIENT = RedisClient.create(REDIS_STANDALONE.getUri());
    REDIS_STANDALONE_CONNECTION = REDIS_STANDALONE_CLIENT.connect();
    REDIS_STANDALONE_COMMANDS = REDIS_STANDALONE_CONNECTION.reactive();

    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(REDIS_CLUSTER.getUri());
    REDIS_CLUSTER_CONNECTION = REDIS_CLUSTER_CLIENT.connect();
    REDIS_CLUSTER_COMMANDS = REDIS_CLUSTER_CONNECTION.reactive();
  }

  @AfterEach
  public void cleanupEach() {
    REDIS_STANDALONE_COMMANDS.flushall().block();
    REDIS_CLUSTER_COMMANDS.flushall().block();
  }

  @AfterAll
  static void cleanupAll() {
    REDIS_STANDALONE_CONNECTION.close();
    REDIS_STANDALONE_CLIENT.shutdown();

    REDIS_CLUSTER_CONNECTION.close();
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  @Test
  public void writeSetCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }

  @Test
  public void writeSetCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireSecondsCommandAppliesCommandToStandalone() {
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

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireSecondsCommandAppliesCommandToCluster() {
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

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireMillisecondsCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.PX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(pttl -> pttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireMillisecondsCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.PX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(pttl -> pttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireKeepTtlCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";

    final Mono<String> result = REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90", new SetArgs().px(2100));

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.KEEPTTL)
                .build())
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(result)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(pttl -> pttl <= 2100)
        .verifyComplete();
  }

  @Test
  public void writeSetWithExpireKeepTtlCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";

    final Mono<String> result = REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90", new SetArgs().px(2100));

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.KEEPTTL)
                .build())
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(result)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(pttl -> pttl <= 2100)
        .verifyComplete();
  }

  @Test
  public void writeSetWithNxConditionCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";

    final Mono<String> result = REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(result)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("artistjanitor90")
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNext(-1L)
        .verifyComplete();
  }

  @Test
  public void writeSetWithNxConditionCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";

    final Mono<String> initialSetResult = REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(initialSetResult)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("artistjanitor90")
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNext(-1L)
        .verifyComplete();
  }

  @Test
  public void writeSetWithXxConditionCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";

    final Mono<String> result = REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.XX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(result)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeSetWithXxConditionCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";

    final Mono<String> result = REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(2100L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.XX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(result)
        .expectNext("OK")
        .verifyComplete();

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()))
        .expectNext("jetpackmelon22")
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()))
        .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getExpiration().getTime())
        .verifyComplete();
  }

  @Test
  public void writeExpireCommandAppliesCommandToStandalone() {
    final RedisExpireCommand redisCommand = RedisExpireCommand.builder()
      .payload(RedisExpireCommand.Payload.builder()
        .key("product.milk")
        .seconds(25L)
        .build())
      .build();

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getSeconds())
      .verifyComplete();
  }

  @Test
  public void writeExpireCommandAppliesCommandToCluster() {
    final RedisExpireCommand redisCommand = RedisExpireCommand.builder()
      .payload(RedisExpireCommand.Payload.builder()
        .key("product.milk")
        .seconds(25L)
        .build())
      .build();

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(ttl -> ttl <= redisCommand.getPayload().getSeconds())
      .verifyComplete();
  }

  @Test
  public void writeExpireatCommandAppliesCommandToStandalone() {
    final long secondsIntoFuture = 50L;
    final long timestamp = Instant.now().plusSeconds(secondsIntoFuture).getEpochSecond();
    final RedisExpireatCommand redisCommand = RedisExpireatCommand.builder()
      .payload(RedisExpireatCommand.Payload.builder()
        .key("product.milk")
        .timestamp(timestamp)
        .build())
      .build();

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(ttl -> ttl <= secondsIntoFuture)
      .verifyComplete();
  }

  @Test
  public void writeExpireatCommandAppliesCommandToCluster() {
    final long secondsIntoFuture = 50L;
    final long timestamp = Instant.now().plusSeconds(secondsIntoFuture).getEpochSecond();
    final RedisExpireatCommand redisCommand = RedisExpireatCommand.builder()
      .payload(RedisExpireatCommand.Payload.builder()
        .key("product.milk")
        .timestamp(timestamp)
        .build())
      .build();

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(ttl -> ttl <= secondsIntoFuture)
      .verifyComplete();
  }

  @Test
  public void writePexpireCommandAppliesCommandToStandalone() {
    final RedisPexpireCommand redisCommand = RedisPexpireCommand.builder()
      .payload(RedisPexpireCommand.Payload.builder()
        .key("product.milk")
        .milliseconds(2500L)
        .build())
      .build();

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(pttl -> pttl <= redisCommand.getPayload().getMilliseconds())
      .verifyComplete();
  }

  @Test
  public void writePexpireCommandAppliesCommandToCluster() {
    final RedisPexpireCommand redisCommand = RedisPexpireCommand.builder()
      .payload(RedisPexpireCommand.Payload.builder()
        .key("product.milk")
        .milliseconds(2500L)
        .build())
      .build();

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29").block();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()))
      .expectNextMatches(pttl -> pttl <= redisCommand.getPayload().getMilliseconds())
      .verifyComplete();
  }

  @Test
  public void writeSaddCommandAppliesCommandToStandalone() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList("fishing", "sport", "tug"))
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.smembers(redisCommand.getPayload().getKey()))
        .thenConsumeWhile(member -> redisCommand.getPayload().getValues().contains(member))
        .verifyComplete();
  }

  @Test
  public void writeSaddCommandAppliesCommandToCluster() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList("fishing", "sport", "tug"))
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.smembers(redisCommand.getPayload().getKey()))
        .thenConsumeWhile(member -> redisCommand.getPayload().getValues().contains(member))
        .verifyComplete();
  }

  @Test
  public void writeGeoaddCommandAppliesCommandToStandalone() {
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

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    // Beware, Redis does not store exact coordinates

    StepVerifier
        .create(REDIS_STANDALONE_COMMANDS.geopos(
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
  public void writeGeoaddCommandAppliesCommandToCluster() {
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

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    StepVerifier
        .create(REDIS_CLUSTER_COMMANDS.geopos(
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
  public void writeGeoaddCommandRetunsErrorForMissingMember() {
    final RedisGeoaddCommand redisCommand = RedisGeoaddCommand.builder()
        .payload(RedisGeoaddCommand.Payload.builder()
            .key("Sicily")
            .values(Arrays.asList(
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(13.361389d)
                    .latitude(38.115556d)
                    .build(),
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .longitude(15.087269d)
                    .latitude(37.502669d)
                    .member("Catania")
                    .build()
            ))
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .expectErrorMatches(exception ->
            exception instanceof ConnectException
                && exception.getMessage().equals("geoadd command does not contain member"))
        .verify();
  }

  @Test
  public void writeArbitraryCommandAppliesCommandToStandalone() {
    final RedisArbitraryCommand redisCommand = RedisArbitraryCommand.builder()
      .payload(RedisArbitraryCommand.Payload.builder()
        .command("SET")
        .arguments(Arrays.asList("arbitrarykey", "arbitraryvalue", "EX", "25"))
        .build())
      .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_COMMANDS.get("arbitrarykey"))
      .expectNext("arbitraryvalue")
      .verifyComplete();

    StepVerifier
      .create(REDIS_STANDALONE_COMMANDS.ttl("arbitrarykey"))
      .expectNextMatches(ttl -> ttl <= 25L)
      .verifyComplete();
  }

  @Test
  public void writeArbitraryCommandAppliesCommandToCluster() {
    final RedisArbitraryCommand redisCommand = RedisArbitraryCommand.builder()
      .payload(RedisArbitraryCommand.Payload.builder()
        .command("SET")
        .arguments(Arrays.asList("arbitrarykey", "arbitraryvalue", "EX", "25"))
        .build())
      .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_COMMANDS.get("arbitrarykey"))
      .expectNext("arbitraryvalue")
      .verifyComplete();

    StepVerifier
      .create(REDIS_CLUSTER_COMMANDS.ttl("arbitrarykey"))
      .expectNextMatches(ttl -> ttl <= 25L)
      .verifyComplete();
  }
}
