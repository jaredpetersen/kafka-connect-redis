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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private long maxPollSize;

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

    maxPollSize = config.getMaxPollRecords();
    LOG.info("Using max poll size of {}", maxPollSize);

    // Set up the subscriber for Redis
    if (config.isRedisClusterEnabled()) {
      LOG.info("Creating cluster Redis client");
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
    final AtomicBoolean breakTask = new AtomicBoolean(false);
    final ConcurrentLinkedQueue<SourceRecord> sourceRecords = new ConcurrentLinkedQueue<>();

    while (!breakTask.get()) {
      final CompletableFuture<RedisMessage> redisMessageFut =
        CompletableFuture.supplyAsync(() -> redisSubscriber.poll());
      CompletableFuture<SourceRecord> sourceRecordFut =
        redisMessageFut.thenApply(
          redisMessage ->
            Optional.ofNullable(redisMessage)
              .map(recordConverter::convert)
              .orElseGet(() -> {
                // No more events left, stop iterating
                breakTask.set(true);
                return null;
              }));

      CompletableFuture<SourceRecord> recordAddFut =
        sourceRecordFut.whenComplete(
          (sourceRecord, exception) -> {
            if (exception != null) {
              throw new ConnectException("failed to convert redis message", exception);
            }
            else if (sourceRecord != null) {
              sourceRecords.offer(sourceRecord);
              if (sourceRecords.size() >= maxPollSize) {
                breakTask.set(true);
              }
            }
          });
      recordAddFut.join();
    }

    if (sourceRecords.size() >= 1) {
      LOG.info("Writing {} record(s) to kafka", sourceRecords.size());
    }

    return new ArrayList<>(sourceRecords);
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
