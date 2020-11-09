package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.sink.config.RedisSinkConfig;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Entry point for Kafka Connect Redis Sink.
 */
public class RedisSinkConnector extends SinkConnector {
  private Map<String, String> config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(final Map<String, String> props) {
    this.config = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedisSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (int configIndex = 0; configIndex < maxTasks; ++configIndex) {
      taskConfigs.add(this.config);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {
    // Do nothing
  }

  @Override
  public ConfigDef config() {
    return RedisSinkConfig.CONFIG_DEF;
  }
}
