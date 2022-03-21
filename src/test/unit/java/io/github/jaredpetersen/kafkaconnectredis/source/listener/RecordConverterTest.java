package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordConverterTest {
  @Test
  void convertTransformsRedisMessageToSourceRecord() {
    final String nodeId = UUID.randomUUID().toString();
    final RedisMessage redisMessage = RedisMessage.builder()
      .nodeId(nodeId)
      .channel("mychannel")
      .pattern("mypattern")
      .message("some message")
      .build();
    final String topic = "mytopic";

    final RecordConverter recordConverter = new RecordConverter(topic);
    final SourceRecord sourceRecord = recordConverter.convert(redisMessage);

    assertTrue(sourceRecord.sourcePartition().isEmpty());
    assertTrue(sourceRecord.sourceOffset().isEmpty());
    assertEquals(topic, sourceRecord.topic());
    assertNull(sourceRecord.kafkaPartition());
    assertEquals(Schema.Type.STRUCT, sourceRecord.keySchema().type());
    assertEquals(redisMessage.getNodeId(), ((Struct) sourceRecord.key()).getString("nodeId"));
    assertEquals(redisMessage.getChannel(), ((Struct) sourceRecord.key()).getString("channel"));
    assertEquals(redisMessage.getPattern(), ((Struct) sourceRecord.key()).getString("pattern"));
    assertEquals(Schema.Type.STRUCT, sourceRecord.valueSchema().type());
    assertEquals(redisMessage.getMessage(), ((Struct) sourceRecord.value()).getString("message"));
    assertTrue(sourceRecord.timestamp() <= Instant.now().toEpochMilli());
  }
}
