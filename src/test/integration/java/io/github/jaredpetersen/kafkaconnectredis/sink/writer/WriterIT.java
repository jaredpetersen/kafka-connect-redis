package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisArbitraryCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisExpireatCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisPexpireCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.github.jaredpetersen.kafkaconnectredis.testutil.RedisContainer;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class WriterIT {
  @Container
  private static final RedisContainer REDIS_STANDALONE = new RedisContainer();

  @Container
  private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterMode();

  private static RedisClient REDIS_STANDALONE_CLIENT;
  private static StatefulRedisConnection<String, String> REDIS_STANDALONE_CONNECTION;
  private static RedisCommands<String, String> REDIS_STANDALONE_COMMANDS;

  private static RedisClusterClient REDIS_CLUSTER_CLIENT;
  private static StatefulRedisClusterConnection<String, String> REDIS_CLUSTER_CONNECTION;
  private static RedisClusterCommands<String, String> REDIS_CLUSTER_COMMANDS;

  @BeforeAll
  static void beforeAll() {
    REDIS_STANDALONE_CLIENT = RedisClient.create(REDIS_STANDALONE.getUri());
    REDIS_STANDALONE_CONNECTION = REDIS_STANDALONE_CLIENT.connect();
    REDIS_STANDALONE_COMMANDS = REDIS_STANDALONE_CONNECTION.sync();

    REDIS_CLUSTER_CLIENT = RedisClusterClient.create(REDIS_CLUSTER.getUri());
    REDIS_CLUSTER_CONNECTION = REDIS_CLUSTER_CLIENT.connect();
    REDIS_CLUSTER_COMMANDS = REDIS_CLUSTER_CONNECTION.sync();
  }

  @AfterEach
  public void afterEach() {
    REDIS_STANDALONE_COMMANDS.flushall();
    REDIS_CLUSTER_COMMANDS.flushall();
  }

  @AfterAll
  static void afterAll() {
    REDIS_STANDALONE_CONNECTION.close();
    REDIS_STANDALONE_CLIENT.shutdown();

    REDIS_CLUSTER_CONNECTION.close();
    REDIS_CLUSTER_CLIENT.shutdown();
  }

  /**
   * Check if the provided TTL is around the expected value. Allows a drift of 2 seconds, i.e. actual TTL can be at
   * most 2 seconds less than the expected TTL.
   *
   * @param ttl TTL (seconds)
   * @param expectedTtl Expected TTL that the ttl should not exceed
   * @return True if TTL is near the expected TTL, given a drift of 2 seconds
   */
  static boolean isValidTtl(long ttl, long expectedTtl) {
    long allowableDrift = 2L;
    return (ttl <= expectedTtl) && (ttl >= (expectedTtl - allowableDrift));
  }

  /**
   * Check if the provided PTTL is around the expected value. Allows a drift of 2,000 milliseconds, i.e. actual PTTL
   * can be at most 2,000 milliseconds less than the expected PTTL.
   *
   * @param pttl PTTL (milliseconds)
   * @param expectedPttl Expected TTL that the ttl should not exceed
   * @return True if PTTL is near the expected PTTL, given a drift of 2,000 milliseconds
   */
  static boolean isValidPttl(long pttl, long expectedPttl) {
    long allowableDrift = 2_000L;
    return (pttl <= expectedPttl) && (pttl >= (expectedPttl - allowableDrift));
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
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
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
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
  }

  @Test
  public void writeSetWithExpireSecondsCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidTtl(
      REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeSetWithExpireSecondsCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidTtl(
      REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeSetWithExpireMillisecondsCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.PX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidPttl(
      REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeSetWithExpireMillisecondsCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.PX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidPttl(
      REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeSetWithExpireKeepTtlCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";
    final long originalPttl = 21_000L;

    REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90", new SetArgs().px(originalPttl));

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
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidPttl(REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()), originalPttl));
  }

  @Test
  public void writeSetWithExpireKeepTtlCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";
    final long originalPttl = 21_000L;

    REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90", new SetArgs().px(originalPttl));

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
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidPttl(REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()), originalPttl));
  }

  @Test
  public void writeSetWithNxConditionCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";

    REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertEquals("artistjanitor90", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertEquals(-1L, REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()));
  }

  @Test
  public void writeSetWithNxConditionCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";

    REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.NX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertEquals("artistjanitor90", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertEquals(-1L, REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()));
  }

  @Test
  public void writeSetWithXxConditionCommandAppliesCommandToStandalone() {
    final String key = "{user.1}.username";

    REDIS_STANDALONE_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.XX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_STANDALONE_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidTtl(
      REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()),
        redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeSetWithXxConditionCommandAppliesCommandToCluster() {
    final String key = "{user.1}.username";

    REDIS_CLUSTER_COMMANDS.set(key, "artistjanitor90");

    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key(key)
            .value("jetpackmelon22")
            .expiration(RedisSetCommand.Payload.Expiration.builder()
                .type(RedisSetCommand.Payload.Expiration.Type.EX)
                .time(21_000L)
                .build())
            .condition(RedisSetCommand.Payload.Condition.XX)
            .build())
        .build();

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertEquals("jetpackmelon22", REDIS_CLUSTER_COMMANDS.get(redisCommand.getPayload().getKey()));
    assertTrue(isValidTtl(
      REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getExpiration().getTime()));
  }

  @Test
  public void writeExpireCommandAppliesCommandToStandalone() {
    final RedisExpireCommand redisCommand = RedisExpireCommand.builder()
      .payload(RedisExpireCommand.Payload.builder()
        .key("product.milk")
        .seconds(25L)
        .build())
      .build();

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidTtl(
      REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getSeconds()));
  }

  @Test
  public void writeExpireCommandAppliesCommandToCluster() {
    final RedisExpireCommand redisCommand = RedisExpireCommand.builder()
      .payload(RedisExpireCommand.Payload.builder()
        .key("product.milk")
        .seconds(25L)
        .build())
      .build();

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidTtl(
      REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getSeconds()));
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

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidTtl(
      REDIS_STANDALONE_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      secondsIntoFuture));
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

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidTtl(
      REDIS_CLUSTER_COMMANDS.ttl(redisCommand.getPayload().getKey()),
      secondsIntoFuture));
  }

  @Test
  public void writePexpireCommandAppliesCommandToStandalone() {
    final RedisPexpireCommand redisCommand = RedisPexpireCommand.builder()
      .payload(RedisPexpireCommand.Payload.builder()
        .key("product.milk")
        .milliseconds(2500L)
        .build())
      .build();

    REDIS_STANDALONE_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidPttl(
      REDIS_STANDALONE_COMMANDS.pttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getMilliseconds()));
  }

  @Test
  public void writePexpireCommandAppliesCommandToCluster() {
    final RedisPexpireCommand redisCommand = RedisPexpireCommand.builder()
      .payload(RedisPexpireCommand.Payload.builder()
        .key("product.milk")
        .milliseconds(2500L)
        .build())
      .build();

    REDIS_CLUSTER_COMMANDS.set(redisCommand.getPayload().getKey(), "$2.29");

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    assertTrue(isValidPttl(
      REDIS_CLUSTER_COMMANDS.pttl(redisCommand.getPayload().getKey()),
      redisCommand.getPayload().getMilliseconds()));
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
    writer.write(redisCommand);

    assertEquals(new HashSet<>(redisCommand.getPayload().getValues()),
      REDIS_STANDALONE_COMMANDS.smembers(redisCommand.getPayload().getKey()));
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
    writer.write(redisCommand);

    assertEquals(new HashSet<>(redisCommand.getPayload().getValues()),
      REDIS_CLUSTER_COMMANDS.smembers(redisCommand.getPayload().getKey()));
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

    // Beware, Redis does not store exact coordinates
    final List<GeoCoordinates> expectedGeoCoordinates = new ArrayList<>();
    expectedGeoCoordinates.add(GeoCoordinates.create(13.36138933897018433d, 38.11555639549629859d));
    expectedGeoCoordinates.add(GeoCoordinates.create(15.087267458438873d, 37.50266842333162d));

    final Writer writer = new Writer(REDIS_STANDALONE_COMMANDS);
    writer.write(redisCommand);

    final List<GeoCoordinates> geoCoordinates = REDIS_STANDALONE_COMMANDS.geopos(
      redisCommand.getPayload().getKey(),
      redisCommand.getPayload().getValues().get(0).getMember(),
      redisCommand.getPayload().getValues().get(1).getMember());

    assertEquals(expectedGeoCoordinates, geoCoordinates);
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

    // Beware, Redis does not store exact coordinates
    final List<GeoCoordinates> expectedGeoCoordinates = new ArrayList<>();
    expectedGeoCoordinates.add(GeoCoordinates.create(13.36138933897018433d, 38.11555639549629859d));
    expectedGeoCoordinates.add(GeoCoordinates.create(15.087267458438873d, 37.50266842333162d));

    final Writer writer = new Writer(REDIS_CLUSTER_COMMANDS);
    writer.write(redisCommand);

    final List<GeoCoordinates> geoCoordinates = REDIS_CLUSTER_COMMANDS.geopos(
      redisCommand.getPayload().getKey(),
      redisCommand.getPayload().getValues().get(0).getMember(),
      redisCommand.getPayload().getValues().get(1).getMember());

    assertEquals(expectedGeoCoordinates, geoCoordinates);
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
    final ConnectException exception = assertThrows(ConnectException.class, () -> writer.write(redisCommand));

    assertEquals("geoadd command does not contain member", exception.getMessage());
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
    writer.write(redisCommand);

    assertEquals("arbitraryvalue", REDIS_STANDALONE_COMMANDS.get("arbitrarykey"));
    assertTrue(REDIS_STANDALONE_COMMANDS.ttl("arbitrarykey") <= 25L);
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
    writer.write(redisCommand);

    assertEquals("arbitraryvalue", REDIS_CLUSTER_COMMANDS.get("arbitrarykey"));
    assertTrue(REDIS_CLUSTER_COMMANDS.ttl("arbitrarykey") <= 25L);
  }
}
