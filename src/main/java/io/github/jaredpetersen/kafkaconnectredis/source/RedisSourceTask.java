package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.source.config.RedisSourceConfig;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.RecordConverter;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisChannelSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisClusterChannelSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisClusterPatternSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisPatternSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisSubscriber;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * Kafka Connect Task for Kafka Connect Redis Sink.
 */
@Slf4j
public class RedisSourceTask extends SourceTask {
  private static final long MAX_POLL_SIZE = 10_000L;

  private RedisClient redisStandaloneClient;
  private StatefulRedisPubSubConnection<String, String> redisStandalonePubSubConnection;

  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;

  private RedisSubscriber redisSubscriber;
  private RecordConverter recordConverter;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    // Map the task properties to config object
    final RedisSourceConfig config;

    try {
      config = new RedisSourceConfig(props);
    }
    catch (ConfigException configException) {
      throw new ConnectException("task configuration error", configException);
    }

    // Set up the subscriber for Redis
    if (config.isRedisClusterEnabled()) {
      redisClusterClient = RedisClusterClient.create(config.getRedisUri());
      redisClusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
          .enableAllAdaptiveRefreshTriggers()
          .enablePeriodicRefresh()
          .build())
        .build());

      redisClusterPubSubConnection = redisClusterClient.connectPubSub();
      redisClusterPubSubConnection.setNodeMessagePropagation(true);

      redisSubscriber = (config.isRedisChannelPatternEnabled())
        ? new RedisClusterPatternSubscriber(
            redisClusterPubSubConnection,
            config.getRedisChannels())
        : new RedisClusterChannelSubscriber(
            redisClusterPubSubConnection,
            config.getRedisChannels());
    }
    else {
      redisStandaloneClient = RedisClient.create(config.getRedisUri());
      redisStandalonePubSubConnection = redisStandaloneClient.connectPubSub();

      redisSubscriber = (config.isRedisChannelPatternEnabled())
        ? new RedisPatternSubscriber(
            redisStandalonePubSubConnection,
            config.getRedisChannels())
        : new RedisChannelSubscriber(
            redisStandalonePubSubConnection,
            config.getRedisChannels());
    }

    recordConverter = new RecordConverter(config.getTopic());
  }

  @Override
  public List<SourceRecord> poll() {
    final List<SourceRecord> sourceRecords = new ArrayList<>();

    while (true) {
      final RedisMessage redisMessage = redisSubscriber.poll();

      // No more events left, stop iterating
      if (redisMessage == null) {
        break;
      }

      final SourceRecord sourceRecord;

      try {
        sourceRecord = recordConverter.convert(redisMessage);
      }
      catch (Exception exception) {
        throw new ConnectException("failed to convert redis message", exception);
      }

      sourceRecords.add(sourceRecord);

      // Subscription events may come in faster than we can iterate over them here so return early once we hit the max
      if (sourceRecords.size() >= MAX_POLL_SIZE) {
        break;
      }
    }

    if (sourceRecords.size() > 1) {
      LOG.info("writing {} record(s) to kafka", sourceRecords.size());
    }

    return sourceRecords;
  }

  @Override
  public void stop() {
    // Close out Redis standalone
    if (redisStandalonePubSubConnection != null) {
      redisStandalonePubSubConnection.close();
    }
    if (redisStandaloneClient != null) {
      redisStandaloneClient.shutdown();
    }

    // Close out Redis cluster
    if (redisClusterPubSubConnection != null) {
      redisClusterPubSubConnection.close();
    }
    if (redisClusterClient != null) {
      redisClusterClient.shutdown();
    }
  }
}
