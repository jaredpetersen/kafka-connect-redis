package io.github.jaredpetersen.kafkaconnectredis.sink.writer.record;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.util.List;

@Value
@Builder(builderClassName = "Builder")
public class RedisGeoaddCommand implements RedisCommand {
  Command command = Command.GEOADD;
  Payload payload;

  @Value
  @lombok.Builder(builderClassName = "Builder")
  public static class Payload {
    String key;
    List<GeoLocation> values;

    @Value
    @lombok.Builder(builderClassName = "Builder")
    public static class GeoLocation {
      BigDecimal latitude;
      BigDecimal longitude;
      String member;
    }
  }
}
