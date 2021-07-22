package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.NodeSelectionPubSubCommands;
import io.lettuce.core.event.Event;
import io.lettuce.core.event.connection.ConnectEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RedisClusterPatternSubscriberTest {
  @Test
  public void subscriberIssuesPsubscribeOnTopologyChange() {
    final String[] patterns = new String[]{"*casts", "*ooks"};;
    final Event connectEvent = new ConnectEvent("redis://dummy", "dummy");
    final Event topologyChangedEvent = new ClusterTopologyChangedEvent(
      Collections.singletonList(RedisClusterNode.of(UUID.randomUUID().toString())),
      Collections.singletonList(RedisClusterNode.of(UUID.randomUUID().toString())));
    final Flux<Event> redisEventFlux = Flux.just(connectEvent, topologyChangedEvent, connectEvent)
      .delayElements(Duration.ofMillis(1000));

    final StatefulRedisClusterPubSubConnection<String, String> redisConnectionMock =
      mock(StatefulRedisClusterPubSubConnection.class, RETURNS_DEEP_STUBS);
    when(redisConnectionMock.sync().upstream().commands()).thenReturn(mock(NodeSelectionPubSubCommands.class));
    when(redisConnectionMock.getResources().eventBus().get()).thenReturn(redisEventFlux);

    new RedisClusterPatternSubscriber(
      redisConnectionMock,
      Arrays.asList(patterns));

    // Should issue psubscribe immediately and then once again after the event is consumed
    // Remember, mock invocations are cumulative so we're asserting this was called a total of 2 times
    verify(redisConnectionMock.sync().upstream().commands()).psubscribe(patterns);
    verify(redisConnectionMock.sync().upstream().commands(), after(5000L).times(2)).psubscribe(patterns);
  }
}
