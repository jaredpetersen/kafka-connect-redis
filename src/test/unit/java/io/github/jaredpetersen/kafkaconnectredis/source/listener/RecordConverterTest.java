package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.time.Instant;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class RecordConverterTest {
  @Test
  public void convertTransformsRedisMessageToSourceRecord() {
    final RedisSubscriptionMessage redisSubscriptionMessage = RedisSubscriptionMessage.builder()
      .channel("mychannel")
      .pattern("mypattern")
      .message("some message")
      .build();
    final String topic = "mytopic";

    final RecordConverter recordConverter = new RecordConverter(topic);

    StepVerifier
      .create(recordConverter.convert(redisSubscriptionMessage))
      .expectNextMatches(sourceRecord ->
        sourceRecord.sourcePartition().size() == 0
          && sourceRecord.sourceOffset().size() == 0
          && sourceRecord.topic().equals(topic)
          && sourceRecord.kafkaPartition() == null
          && sourceRecord.keySchema().type() == Schema.Type.STRUCT
          && ((Struct) sourceRecord.key()).getString("channel").equals("mychannel")
          && ((Struct) sourceRecord.key()).getString("pattern").equals("mypattern")
          && sourceRecord.valueSchema().type() == Schema.Type.STRUCT
          && ((Struct) sourceRecord.value()).getString("message").equals("some message")
          && sourceRecord.timestamp() <= Instant.now().getEpochSecond());
  }
}
