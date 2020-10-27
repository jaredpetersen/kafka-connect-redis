package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.SubscriptionEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.Mono;

public class RecordConverter {
  private final String topic;

  private static final Schema KEY_SCHEMA = SchemaBuilder.string()
    .name("RedisSubscriptionEventKey");
  private static final Schema VALUE_SCHEMA = SchemaBuilder.string()
    .name("RedisSubscriptionEventValue");

  /**
   * Set up converter with preset topic.
   *
   * @param topic Topic to use for all converted records.
   */
  public RecordConverter(String topic) {
    this.topic = topic;
  }

  /**
   * Convert subscription event to source record.
   *
   * @param event Subscription event to be converted.
   * @return Converted source record.
   */
  public Mono<SourceRecord> convert(SubscriptionEvent<String, String> event) {
    // Source partition and offset are not useful in our case because the Redis subscription model does not allow us
    // to pick up where we left off if we stop subscribing for a while
    final Map<String, ?> sourcePartition = new HashMap<>();
    final Map<String, ?> sourceOffset = new HashMap<>();

    final String key = event.getChannel();
    final String value = event.getMessage();

    final SourceRecord sourceRecord = new SourceRecord(
      sourcePartition,
      sourceOffset,
      this.topic,
      KEY_SCHEMA,
      key,
      VALUE_SCHEMA,
      value
    );

    return Mono.just(sourceRecord);
  }
}
