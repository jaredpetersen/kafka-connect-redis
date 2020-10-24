package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.sink.config.RedisSinkConfig;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.Listener;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect Task for Kafka Connect Redis Sink.
 */
public class RedisSourceTask extends SourceTask {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSourceTask.class);

  private RedisClient redisStandaloneClient;
  private StatefulRedisConnection<String, String> redisStandaloneConnection;

  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterConnection<String, String> redisClusterConnection;

  private Listener listener;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    // Map the task properties to config object
    final RedisSinkConfig config = new RedisSinkConfig(props);

    if (config.isRedisClusterEnabled()) {
      this.redisClusterClient = RedisClusterClient.create(config.getRedisUri());
      this.redisClusterConnection = this.redisClusterClient.connect();

      final RedisClusterReactiveCommands<String, String> redisClusterCommands = this.redisClusterConnection.reactive();
      this.listener = new Listener(redisClusterCommands);
    }
    else {
      this.redisStandaloneClient = RedisClient.create(config.getRedisUri());
      this.redisStandaloneConnection = this.redisStandaloneClient.connect();

      final RedisReactiveCommands<String, String> redisStandaloneCommands = this.redisStandaloneConnection.reactive();
      this.listener = new Listener(redisStandaloneCommands);
    }
  }

  @Override
  public List<SourceRecord> poll() {
    return this.listener.poll();
  }

  @Override
  public void stop() {
    if (this.redisStandaloneConnection != null) {
      this.redisStandaloneConnection.close();
    }
    if (this.redisStandaloneClient != null) {
      this.redisStandaloneClient.shutdown();
    }

    if (this.redisClusterConnection != null) {
      this.redisClusterConnection.close();
    }
    if (this.redisClusterClient != null) {
      this.redisClusterClient.shutdown();
    }
  }
}
