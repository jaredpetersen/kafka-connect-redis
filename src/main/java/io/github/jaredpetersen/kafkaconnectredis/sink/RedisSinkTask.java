package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.sink.config.RedisSinkConfig;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.Writer;
import io.github.jaredpetersen.kafkaconnectredis.sink.writer.RecordConverter;
import io.github.jaredpetersen.kafkaconnectredis.util.VersionUtil;

import java.util.Collection;
import java.util.Map;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Kafka Connect Task for Kafka Connect Redis Sink.
 */
public class RedisSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSinkTask.class);

  private static final RecordConverter RECORD_CONVERTER = new RecordConverter();

  private RedisClient redisClient;
  private Writer writer;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(final Map<String, String> props) {
    LOG.info("task config: {}", props);

    // Map the task properties to config object
    final RedisSinkConfig config = new RedisSinkConfig(props);

    this.redisClient = RedisClient.create(config.getRedisUri());
    final RedisReactiveCommands<String, String> redisCommands = this.redisClient.connect().reactive();

    this.writer = new Writer(redisCommands);
  }

  @Override
  public void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    LOG.info("writing {} record(s)", records.size());

    Flux
        .fromIterable(records)
        .flatMapSequential(RECORD_CONVERTER::convert)
        .doOnError(error -> LOG.error("failed to convert record", error))
        .flatMapSequential(redisCommand -> this.writer.write(redisCommand))
        .doOnError(error -> LOG.error("failed to write record", error))
        .then()
        .block();
  }

  @Override
  public void stop() {
    this.redisClient.shutdown();
  }
}
