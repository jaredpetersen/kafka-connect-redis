package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.Mono;

public class RecordConverter {
  private final String topic;

  private static final Schema KEY_SCHEMA = SchemaBuilder.string()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventKey");
  private static final Schema VALUE_SCHEMA = SchemaBuilder.string()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventValue");

  /**
   * Set up converter with preset topic.
   *
   * @param topic Topic to use for all converted records.
   */
  public RecordConverter(String topic) {
    this.topic = topic;
  }

  /**
   * Convert subscription message to source record.
   *
   * @param redisMessage Redis subscription event to be converted.
   * @return Converted source record.
   */
  public Mono<SourceRecord> convert(RedisSubscriptionMessage redisMessage) {
    // Source partition and offset are not useful in our case because the Redis subscription model does not allow us
    // to pick up where we left off if we stop subscribing for a while
    final Map<String, ?> sourcePartition = new HashMap<>();
    final Map<String, ?> sourceOffset = new HashMap<>();

    final int partition = 0;
    final String key = redisMessage.getChannel();
    final String value = redisMessage.getMessage();

    final SourceRecord sourceRecord = new SourceRecord(
      sourcePartition,
      sourceOffset,
      this.topic,
      partition,
      KEY_SCHEMA,
      key,
      VALUE_SCHEMA,
      value,
      Instant.now().toEpochMilli()
    );

    return Mono.just(sourceRecord);
  }
}
