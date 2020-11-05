package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.source.config.RedisSourceConfig;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.*;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Kafka Connect Task for Kafka Connect Redis Sink.
 */
public class RedisSourceTask extends SourceTask {
  private RedisClient redisStandaloneClient;
  private StatefulRedisPubSubConnection<String, String> redisStandalonePubSubConnection;

  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterPubSubConnection<String, String> redisClusterPubSubConnection;

  private RecordConverter recordConverter;

  private RedisListener listener;

  private static final Logger LOG = LoggerFactory.getLogger(RedisSourceTask.class);

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    // Map the task properties to config object
    final RedisSourceConfig config = new RedisSourceConfig(props);

    if (config.isRedisClusterEnabled()) {
      this.redisClusterClient = RedisClusterClient.create(config.getRedisUri());
      this.redisClusterPubSubConnection = this.redisClusterClient.connectPubSub();
      this.redisClusterPubSubConnection.setNodeMessagePropagation(true);

      this.listener = (config.isRedisChannelPatternEnabled())
        ? new RedisClusterChannelListener(
            redisClusterPubSubConnection.reactive(),
            config.getRedisChannels())
        : new RedisClusterPatternListener(
            redisClusterPubSubConnection.reactive(),
            config.getRedisChannels());
    }
    else {
      this.redisStandaloneClient = RedisClient.create(config.getRedisUri());
      this.redisStandalonePubSubConnection = this.redisStandaloneClient.connectPubSub();

      this.listener = (config.isRedisChannelPatternEnabled())
        ? new RedisChannelListener(
            redisClusterPubSubConnection.reactive(),
            config.getRedisChannels())
        : new RedisPatternListener(
            redisClusterPubSubConnection.reactive(),
            config.getRedisChannels());
    }

    this.listener.start();

    this.recordConverter = new RecordConverter(config.getTopic());
  }

  @Override
  public List<SourceRecord> poll() {
    final List<SourceRecord> sourceRecords = Flux
      .fromIterable(this.listener.poll())
      .flatMapSequential(this.recordConverter::convert)
      .collectList()
      .block();

    if (sourceRecords.size() > 1) {
      LOG.info("writing {} record(s) to kafka", sourceRecords.size());
    }

    return sourceRecords;
  }

  @Override
  public void stop() {
    this.listener.stop();

    // Close out Redis standalone
    if (this.redisStandalonePubSubConnection != null) {
      this.redisStandalonePubSubConnection.close();
    }
    if (this.redisStandaloneClient != null) {
      this.redisStandaloneClient.shutdown();
    }

    // Close out Redis cluster
    if (this.redisClusterPubSubConnection != null) {
      this.redisClusterPubSubConnection.close();
    }
    if (this.redisClusterClient != null) {
      this.redisClusterClient.shutdown();
    }
  }
}
