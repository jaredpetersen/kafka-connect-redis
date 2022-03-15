package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class RecordConverter {
  private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventKey")
    .field("channel", Schema.STRING_SCHEMA)
    .field("pattern", Schema.OPTIONAL_STRING_SCHEMA);
  private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
    .name("io.github.jaredpetersen.kafkaconnectredis.RedisSubscriptionEventValue")
    .field("message", Schema.STRING_SCHEMA);

  private final String topic;

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
  public SourceRecord convert(RedisMessage redisMessage) {
    // Source partition and offset are not useful in our case because the Redis subscription model does not allow us
    // to pick up where we left off if we stop subscribing for a while
    final Map<String, ?> sourcePartition = new HashMap<>();
    final Map<String, ?> sourceOffset = new HashMap<>();

    final Struct key = new Struct(KEY_SCHEMA)
      .put("channel", redisMessage.getChannel())
      .put("pattern", redisMessage.getPattern());
    final Struct value = new Struct(VALUE_SCHEMA)
      .put("message", redisMessage.getMessage());

    return new SourceRecord(
      sourcePartition,
      sourceOffset,
      this.topic,
      null,
      KEY_SCHEMA,
      key,
      VALUE_SCHEMA,
      value,
      Instant.now().toEpochMilli()
    );
  }
}
