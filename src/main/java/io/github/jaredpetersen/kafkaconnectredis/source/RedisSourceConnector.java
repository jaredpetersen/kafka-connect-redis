package io.github.jaredpetersen.kafkaconnectredis.source;

import io.github.jaredpetersen.kafkaconnectredis.source.config.RedisSourceConfig;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

/**
 * Entry point for Kafka Connect Redis Sink.
 */
public class RedisSourceConnector extends SourceConnector {
  private RedisSourceConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(final Map<String, String> props) {
    // Map the connector properties to config object
    try {
      this.config = new RedisSourceConfig(props);
    }
    catch (ConfigException configException) {
      throw new ConnectException("connector configuration error", configException);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedisSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int
                                                   maxTasks) {
    // Partition the configs based on channels
    final List<List<String>> partitionedRedisChannels = ConnectorUtils
      .groupPartitions(this.config.getRedisChannels(), Math.min(this.config.getRedisChannels().size(), maxTasks));

    // Create task configs based on the partitions
    return partitionedRedisChannels.stream()
      .map(redisChannels -> {
        final Map<String, String> taskConfig = new HashMap<>(this.config.originalsStrings());
        taskConfig.put(RedisSourceConfig.REDIS_CHANNELS, String.join(",", redisChannels));

        return taskConfig;
      })
      .collect(Collectors.toList());
  }

  @Override
  public void stop() {
    // Do nothing
  }

  @Override
  public ConfigDef config() {
    return RedisSourceConfig.CONFIG_DEF;
  }
}
