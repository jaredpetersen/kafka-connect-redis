package io.github.jaredpetersen.kafkaconnectredis.sink;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisListener;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisSubscriptionMessage;
import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisSubscriber;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class RedisListenerTest {
  @Test
  public void startSubscribesAndListens() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisSubscriptionMessage redisMessage = RedisSubscriptionMessage.builder()
          .channel("election")
          .message("vote-" + state)
          .build();

        sink.next(redisMessage);

        return ++state;
      }
    );

    final RedisSubscriber mockRedisSubscriber = mock(RedisSubscriber.class);
    when(mockRedisSubscriber.subscribe()).thenReturn(Mono.empty());
    when(mockRedisSubscriber.observe()).thenReturn(redisSubscriptionMessageFlux);

    final RedisListener redisListener = new RedisListener(mockRedisSubscriber);

    redisListener.start();

    Thread.sleep(1000L);

    assertTrue(redisSubscriptionMessageIndex.get() > 0);
  }

  @Test
  public void stopUnsubscribesAndStopsListening() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisSubscriptionMessage redisMessage = RedisSubscriptionMessage.builder()
          .channel("election")
          .message("vote-" + state)
          .build();

        sink.next(redisMessage);

        return ++state;
      }
    );

    final RedisSubscriber mockRedisSubscriber = mock(RedisSubscriber.class);
    when(mockRedisSubscriber.subscribe()).thenReturn(Mono.empty());
    when(mockRedisSubscriber.observe()).thenReturn(redisSubscriptionMessageFlux);
    when(mockRedisSubscriber.unsubscribe()).thenReturn(Mono.empty());

    final RedisListener redisListener = new RedisListener(mockRedisSubscriber);

    redisListener.start();
    Thread.sleep(1000L);
    redisListener.stop();

    final int redisSubscriptionMessageIndexAfterStop = redisSubscriptionMessageIndex.get();

    Thread.sleep(1000L);

    assertEquals(redisSubscriptionMessageIndexAfterStop, redisSubscriptionMessageIndex.get());
  }

  @Test
  public void pollRetrievesSubscribedMessages() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisSubscriptionMessage redisMessage = RedisSubscriptionMessage.builder()
          .channel("election")
          .message("vote-" + state)
          .build();

        sink.next(redisMessage);

        return ++state;
      }
    );

    final RedisSubscriber mockRedisSubscriber = mock(RedisSubscriber.class);
    when(mockRedisSubscriber.subscribe()).thenReturn(Mono.empty());
    when(mockRedisSubscriber.observe()).thenReturn(redisSubscriptionMessageFlux);
    when(mockRedisSubscriber.unsubscribe()).thenReturn(Mono.empty());

    final RedisListener redisListener = new RedisListener(mockRedisSubscriber);

    redisListener.start();

    // Give the listener time to observe some messages
    Thread.sleep(2000L);

    final List<RedisSubscriptionMessage> redisSubscriptionMessages = redisListener.poll();

    assertTrue(redisSubscriptionMessages.size() > 0);
    assertTrue(redisSubscriptionMessages.size() <= 100_000);
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-0")
        .build(),
      redisSubscriptionMessages.get(0));
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-" + (redisSubscriptionMessages.size() - 1))
        .build(),
      redisSubscriptionMessages.get(redisSubscriptionMessages.size() - 1));
  }

  @Test
  public void pollMultipleTiesRetrievesSubscribedMessagesWithoutDataLoss() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisSubscriptionMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisSubscriptionMessage redisMessage = RedisSubscriptionMessage.builder()
          .channel("election")
          .message("vote-" + state)
          .build();

        sink.next(redisMessage);

        return ++state;
      }
    );

    final RedisSubscriber mockRedisSubscriber = mock(RedisSubscriber.class);
    when(mockRedisSubscriber.subscribe()).thenReturn(Mono.empty());
    when(mockRedisSubscriber.observe()).thenReturn(redisSubscriptionMessageFlux);
    when(mockRedisSubscriber.unsubscribe()).thenReturn(Mono.empty());

    final RedisListener redisListener = new RedisListener(mockRedisSubscriber);

    redisListener.start();

    // Give the listener time to observe some messages
    Thread.sleep(1000L);

    final List<RedisSubscriptionMessage> redisSubscriptionMessagesRoundA = redisListener.poll();

    assertTrue(redisSubscriptionMessagesRoundA.size() > 0);
    assertTrue(redisSubscriptionMessagesRoundA.size() <= 100_000);
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-0")
        .build(),
      redisSubscriptionMessagesRoundA.get(0));
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-" + (redisSubscriptionMessagesRoundA.size() - 1))
        .build(),
      redisSubscriptionMessagesRoundA.get(redisSubscriptionMessagesRoundA.size() - 1));

    // Poll again, confirming that we're getting the next batch of data
    final List<RedisSubscriptionMessage> redisSubscriptionMessagesRoundB = redisListener.poll();

    assertTrue(redisSubscriptionMessagesRoundB.size() > 0);
    assertTrue(redisSubscriptionMessagesRoundB.size() <= 100_000);
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-" + redisSubscriptionMessagesRoundA.size())
        .build(),
      redisSubscriptionMessagesRoundB.get(0));
    assertEquals(
      RedisSubscriptionMessage.builder()
        .channel("election")
        .message("vote-" + (redisSubscriptionMessagesRoundA.size() + redisSubscriptionMessagesRoundB.size() - 1))
        .build(),
      redisSubscriptionMessagesRoundB.get(redisSubscriptionMessagesRoundB.size() - 1));
  }
}
