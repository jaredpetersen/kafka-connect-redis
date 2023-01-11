package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.sink.config.RedisSinkConfig;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.RecordConverter;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.Writer;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.record.RedisCommand;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Kafka Connect Task for Kafka Connect Redis Sink.
 */
@Slf4j
public class RedisSinkTask extends SinkTask {
  private static final String SENTINEL_AUTH_PREFIX = "sentinelauth=";
  private static final RecordConverter RECORD_CONVERTER = new RecordConverter();

  private RedisClient redisStandaloneClient;
  private StatefulRedisConnection<String, String> redisStandaloneConnection;

  // Add redis sentinel connection support
  private StatefulRedisMasterReplicaConnection<String, String> redisSentinelConnection;

  private RedisClusterClient redisClusterClient;
  private StatefulRedisClusterConnection<String, String> redisClusterConnection;

  private Writer writer;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(final Map<String, String> props) {
    // Map the task properties to config object
    final RedisSinkConfig config;

    try {
      config = new RedisSinkConfig(props);
    }
    catch (ConfigException configException) {
      throw new ConnectException("task configuration error");
    }

    // Set up the writer
    if (config.isRedisClusterEnabled()) {
      this.redisClusterClient = RedisClusterClient.create(config.getRedisUri());
      this.redisClusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
          .enableAllAdaptiveRefreshTriggers()
          .enablePeriodicRefresh()
          .build())
        .build());

      this.redisClusterConnection = this.redisClusterClient.connect();

      final RedisClusterCommands<String, String> redisClusterCommands = this.redisClusterConnection.sync();
      this.writer = new Writer(redisClusterCommands);
    }
    else {
      // Add redis sentinel uri connect logic
      // If the sentinel is configured with a password, use the "sentinelAuth" parameter in the url
      if (config.getRedisUri().startsWith("redis-sentinel")) {
        RedisURI redisUri = RedisURI.create(config.getRedisUri());
        URI uri = URI.create(config.getRedisUri());
        // Sentinel password configuration logic
        if (!StringUtil.isNullOrEmpty(uri.getQuery())) {
          StringTokenizer st = new StringTokenizer(uri.getQuery(), "&;");
          // Get the sentinel-auth from url query parameters
          while (st.hasMoreTokens()) {
            String queryParam = st.nextToken();
            String forStartWith = queryParam.toLowerCase();
            if (forStartWith.startsWith(SENTINEL_AUTH_PREFIX)) {
              final String auth = queryParam.substring(SENTINEL_AUTH_PREFIX.length());
              if (!StringUtil.isNullOrEmpty(auth)) {
                redisUri.getSentinels().forEach(redisSentinelUri -> redisSentinelUri.setPassword(auth.toCharArray()));
              }
            }
          }
        }

        this.redisSentinelConnection = MasterReplica.connect(RedisClient.create(), StringCodec.UTF8, redisUri);
        this.redisSentinelConnection.setReadFrom(ReadFrom.REPLICA_PREFERRED);

        final RedisCommands<String, String> redisSentinelCommands = this.redisSentinelConnection.sync();
        this.writer = new Writer(redisSentinelCommands);
      }
      else {
        this.redisStandaloneClient = RedisClient.create(config.getRedisUri());
        this.redisStandaloneConnection = this.redisStandaloneClient.connect();

        final RedisCommands<String, String> redisStandaloneCommands = this.redisStandaloneConnection.sync();
        this.writer = new Writer(redisStandaloneCommands);
      }
    }
  }

  @Override
  public void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    LOG.info("Writing {} record(s) to redis", records.size());
    LOG.debug("Records: {}", records);

    for (SinkRecord record : records) {
      put(record);
    }
  }

  private void put(SinkRecord record) {
    final RedisCommand redisCommand;

    try {
      redisCommand = RECORD_CONVERTER.convert(record);
    }
    catch (Exception exception) {
      throw new ConnectException("failed to convert record", exception);
    }

    try {
      writer.write(redisCommand);
    }
    catch (Exception exception) {
      throw new ConnectException("failed to write record", exception);
    }
  }

  @Override
  public void stop() {
    if (this.redisStandaloneConnection != null) {
      this.redisStandaloneConnection.close();
    }
    if (this.redisStandaloneClient != null) {
      this.redisStandaloneClient.shutdown();
    }

    if (this.redisSentinelConnection != null) {
      this.redisSentinelConnection.close();
    }

    if (this.redisClusterConnection != null) {
      this.redisClusterConnection.close();
    }
    if (this.redisClusterClient != null) {
      this.redisClusterClient.shutdown();
    }
  }
}
