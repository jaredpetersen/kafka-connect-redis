package io.github.jaredpetersen.kafkaconnectredis.source.listener.record;

import java.util.List;
import lombok.Builder;
import lombok.Value;

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
      double latitude;
      double longitude;
      String member;
    }
  }
}
