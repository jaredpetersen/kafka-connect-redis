package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisGeoaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSaddCommand;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisSetCommand;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class WriterTest {
  @Test
  public void writeRedisSetCommandAppliesCommandToStandalone() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
      .payload(RedisSetCommand.Payload.builder()
        .key("{user.1}.username")
        .value("jetpackmelon22")
        .build())
      .build();
    final RedisReactiveCommands<String, String> redisStandaloneCommandsMock = mock(RedisReactiveCommands.class);
    when(redisStandaloneCommandsMock.set(anyString(), anyString(), any(SetArgs.class)))
      .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisStandaloneCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
      .create(write)
      .verifyComplete();

    verify(redisStandaloneCommandsMock)
      .set(eq(redisCommand.getPayload().getKey()), eq(redisCommand.getPayload().getValue()), any(SetArgs.class));
  }

  @Test
  public void writeRedisSetCommandAppliesCommandToCluster() {
    final RedisSetCommand redisCommand = RedisSetCommand.builder()
        .payload(RedisSetCommand.Payload.builder()
            .key("{user.1}.username")
            .value("jetpackmelon22")
            .build())
        .build();
    final RedisClusterReactiveCommands<String, String> redisClusterCommandsMock = mock(RedisClusterReactiveCommands.class);
    when(redisClusterCommandsMock.set(anyString(), anyString(), any(SetArgs.class)))
        .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisClusterCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    verify(redisClusterCommandsMock)
        .set(eq(redisCommand.getPayload().getKey()), eq(redisCommand.getPayload().getValue()), any(SetArgs.class));
  }

  @Test
  public void writeRedisSaddCommandAppliesCommandToStandalone() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
          .key("boats")
          .values(Arrays.asList("fishing", "sport", "tug"))
          .build())
        .build();
    final RedisReactiveCommands<String, String> redisStandaloneCommandsMock = mock(RedisReactiveCommands.class);
    when(redisStandaloneCommandsMock.sadd(anyString(), ArgumentMatchers.<String>any()))
        .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisStandaloneCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    final ArgumentCaptor<String> membersCaptor = ArgumentCaptor.forClass(String.class);
    verify(redisStandaloneCommandsMock)
        .sadd(eq(redisCommand.getPayload().getKey()), membersCaptor.capture());

    assertEquals(redisCommand.getPayload().getValues(), membersCaptor.getAllValues());
  }

  @Test
  public void writeRedisSaddCommandAppliesCommandToCluster() {
    final RedisSaddCommand redisCommand = RedisSaddCommand.builder()
        .payload(RedisSaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList("fishing", "sport", "tug"))
            .build())
        .build();
    final RedisClusterReactiveCommands<String, String> redisClusterCommandsMock = mock(RedisClusterReactiveCommands.class);
    when(redisClusterCommandsMock.sadd(anyString(), ArgumentMatchers.<String>any()))
        .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisClusterCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    final ArgumentCaptor<String> membersCaptor = ArgumentCaptor.forClass(String.class);
    verify(redisClusterCommandsMock)
        .sadd(eq(redisCommand.getPayload().getKey()), membersCaptor.capture());

    assertEquals(redisCommand.getPayload().getValues(), membersCaptor.getAllValues());
  }

  @Test
  public void writeRedisGeoaddCommandAppliesCommandToStandalone() {
    final RedisGeoaddCommand redisCommand = RedisGeoaddCommand.builder()
        .payload(RedisGeoaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList(
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                  .latitude(new BigDecimal("38.115556"))
                  .longitude(new BigDecimal("13.361389"))
                  .member("Palermo")
                  .build(),
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                  .latitude(new BigDecimal("37.502669"))
                  .longitude(new BigDecimal("15.087269"))
                  .member("Catania")
                  .build()
            ))
            .build())
        .build();
    final RedisReactiveCommands<String, String> redisStandaloneCommandsMock = mock(RedisReactiveCommands.class);
    when(redisStandaloneCommandsMock.geoadd(anyString(), ArgumentMatchers.<String>any()))
        .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisStandaloneCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    verify(redisStandaloneCommandsMock)
        .geoadd(
            redisCommand.getPayload().getKey(),
            redisCommand.getPayload().getValues().get(0).getLongitude(),
            redisCommand.getPayload().getValues().get(0).getLatitude(),
            redisCommand.getPayload().getValues().get(0).getMember(),
            redisCommand.getPayload().getValues().get(1).getLongitude(),
            redisCommand.getPayload().getValues().get(1).getLatitude(),
            redisCommand.getPayload().getValues().get(1).getMember());
  }

  @Test
  public void writeRedisGeoaddCommandAppliesCommandToCluster() {
    final RedisGeoaddCommand redisCommand = RedisGeoaddCommand.builder()
        .payload(RedisGeoaddCommand.Payload.builder()
            .key("boats")
            .values(Arrays.asList(
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .latitude(new BigDecimal("38.115556"))
                    .longitude(new BigDecimal("13.361389"))
                    .member("Palermo")
                    .build(),
                RedisGeoaddCommand.Payload.GeoLocation.builder()
                    .latitude(new BigDecimal("37.502669"))
                    .longitude(new BigDecimal("15.087269"))
                    .member("Catania")
                    .build()
            ))
            .build())
        .build();
    final RedisClusterReactiveCommands<String, String> redisClusterCommandsMock = mock(RedisClusterReactiveCommands.class);
    when(redisClusterCommandsMock.geoadd(anyString(), ArgumentMatchers.<String>any()))
        .thenReturn(Mono.empty());

    final Writer writer = new Writer(redisClusterCommandsMock);
    final Mono<Void> write = writer.write(redisCommand);

    StepVerifier
        .create(write)
        .verifyComplete();

    verify(redisClusterCommandsMock)
        .geoadd(
            redisCommand.getPayload().getKey(),
            redisCommand.getPayload().getValues().get(0).getLongitude(),
            redisCommand.getPayload().getValues().get(0).getLatitude(),
            redisCommand.getPayload().getValues().get(0).getMember(),
            redisCommand.getPayload().getValues().get(1).getLongitude(),
            redisCommand.getPayload().getValues().get(1).getLatitude(),
            redisCommand.getPayload().getValues().get(1).getMember());
  }
}
