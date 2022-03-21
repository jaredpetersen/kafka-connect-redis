package io.github.jaredpetersen.kafkaconnectredis.source.config;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class RedisSourceConfig extends AbstractConfig {
  public static final String TOPIC = "topic";
  private static final String TOPIC_DOC = "Topic to write to.";
  private final String topic;

  public static final String REDIS_URI = "redis.uri";
  private static final String REDIS_URI_DOC = "Redis uri.";
  private final String redisUri;

  public static final String REDIS_CLUSTER_ENABLED = "redis.cluster.enabled";
  private static final String REDIS_CLUSTER_ENABLED_DOC = "Redis cluster mode enabled.";
  private static final boolean REDIS_CLUSTER_ENABLED_DEFAULT = false;
  private final boolean redisClusterEnabled;

  public static final String REDIS_CHANNELS = "redis.channels";
  private static final String REDIS_CHANNELS_DOC = "Redis channel(s) to subscribe to, comma-separated.";
  private final List<String> redisChannels;

  public static final String REDIS_CHANNELS_PATTERN_ENABLED = "redis.channels.pattern.enabled";
  private static final String REDIS_CHANNELS_PATTERN_ENABLED_DOC = "Redis channel(s) utilize patterns.";
  private final boolean redisChannelPatternEnabled;

  public static final String MAX_POLL_RECORDS = "max.poll.records";
  private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single "
    + "call to poll(). Note, that <code>max.poll.records</code> does not impact the underlying fetching behavior. "
    + "The consumer will cache the records from each fetch request and returns them incrementally from each poll.";
  private static final long MAX_POLL_RECORDS_DEFAULT = 10_000;
  private final long maxPollRecords;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(
      TOPIC,
      Type.STRING,
      Importance.HIGH,
      TOPIC_DOC)
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
      REDIS_CLUSTER_ENABLED_DOC)
    .define(
      REDIS_CHANNELS,
      Type.LIST,
      Importance.HIGH,
      REDIS_CHANNELS_DOC)
    .define(
      REDIS_CHANNELS_PATTERN_ENABLED,
      Type.BOOLEAN,
      Importance.HIGH,
      REDIS_CHANNELS_PATTERN_ENABLED_DOC)
    .define(
      MAX_POLL_RECORDS,
      Type.LONG,
      MAX_POLL_RECORDS_DEFAULT,
      Importance.MEDIUM,
      MAX_POLL_RECORDS_DOC);

  /**
   * Configuration for Redis Source.
   *
   * @param originals configurations.
   */
  public RedisSourceConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);

    this.topic = getString(TOPIC);
    this.redisUri = getPassword(REDIS_URI).value();
    this.redisClusterEnabled = getBoolean(REDIS_CLUSTER_ENABLED);
    this.redisChannels = getList(REDIS_CHANNELS);
    this.redisChannelPatternEnabled = getBoolean(REDIS_CHANNELS_PATTERN_ENABLED);
    this.maxPollRecords = getLong(MAX_POLL_RECORDS);
  }

  /**
   * Get Topic to write to.
   *
   * @return Topic that can be written to.
   */
  public String getTopic() {
    return this.topic;
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

  /**
   * Get Redis channels to subscribe to.
   *
   * @return Redis channels.
   */
  public List<String> getRedisChannels() {
    return this.redisChannels;
  }

  /**
   * Get Redis pattern matching enablement on channels.
   *
   * @return Redis channels utilize pattern matching.
   */
  public boolean isRedisChannelPatternEnabled() {
    return this.redisChannelPatternEnabled;
  }

  /**
   * Get maximum records in a batch.
   *
   * @return Maximum records in a batch.
   */
  public long getMaxPollRecords() {
    return this.maxPollRecords;
  }
}
