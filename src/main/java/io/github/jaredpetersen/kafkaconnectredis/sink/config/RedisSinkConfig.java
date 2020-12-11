package io.github.jaredpetersen.kafkaconnectredis.sink.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class RedisSinkConfig extends AbstractConfig {
  public static final String REDIS_URI = "redis.uri";
  private static final String REDIS_URI_DOC = "Redis uri.";
  private final String redisUri;

  public static final String REDIS_CLUSTER_ENABLED = "redis.cluster.enabled";
  private static final String REDIS_CLUSTER_ENABLED_DOC = "Redis cluster mode enabled.";
  private static final boolean REDIS_CLUSTER_ENABLED_DEFAULT = false;
  private final boolean redisClusterEnabled;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(
      REDIS_URI,
      Type.PASSWORD,
      Importance.HIGH,
      REDIS_URI_DOC)
    .define(
      REDIS_CLUSTER_ENABLED,
      Type.BOOLEAN,
      REDIS_CLUSTER_ENABLED_DEFAULT,
      Importance.HIGH,
      REDIS_CLUSTER_ENABLED_DOC);

  /**
   * Configuration for Redis Sink.
   *
   * @param originals configurations.
   */
  public RedisSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);

    this.redisUri = getPassword(REDIS_URI).value();
    this.redisClusterEnabled = getBoolean(REDIS_CLUSTER_ENABLED);
  }

  /**
   * Get URI for Redis.
   *
   * @return Redis URI.
   */
  public String getRedisUri() {
    return this.redisUri;
  }

  /**
   * Get Redis cluster enablement status.
   *
   * @return Redis cluster enablement status.
   */
  public boolean isRedisClusterEnabled() {
    return this.redisClusterEnabled;
  }
}
