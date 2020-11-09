package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisSubscriber;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedisListenerTest {
  @Test
  public void startSubscribesAndListens() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisMessage redisMessage = RedisMessage.builder()
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
    final Flux<RedisMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisMessage redisMessage = RedisMessage.builder()
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
  public void pollRetrievesEmptyListOfSubscribedMessages() throws InterruptedException {
    final RedisSubscriber mockRedisSubscriber = mock(RedisSubscriber.class);
    when(mockRedisSubscriber.subscribe()).thenReturn(Mono.empty());
    when(mockRedisSubscriber.observe()).thenReturn(Flux.empty());
    when(mockRedisSubscriber.unsubscribe()).thenReturn(Mono.empty());

    final RedisListener redisListener = new RedisListener(mockRedisSubscriber);

    final List<RedisMessage> redisMessages = redisListener.poll();

    assertEquals(0, redisMessages.size());
  }

  @Test
  public void pollRetrievesSubscribedMessages() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisMessage redisMessage = RedisMessage.builder()
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

    final List<RedisMessage> redisMessages = redisListener.poll();

    assertTrue(redisMessages.size() > 0);
    assertTrue(redisMessages.size() <= 100_000);
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-0")
        .build(),
      redisMessages.get(0));
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-" + (redisMessages.size() - 1))
        .build(),
      redisMessages.get(redisMessages.size() - 1));
  }

  @Test
  public void pollMultipleTiesRetrievesSubscribedMessagesWithoutDataLoss() throws InterruptedException {
    // Generate an unbounded flux redis messages on demand
    // Use an external index to track the progress for verification purposes
    final AtomicInteger redisSubscriptionMessageIndex = new AtomicInteger();
    final Flux<RedisMessage> redisSubscriptionMessageFlux = Flux.generate(
      () -> 0,
      (state, sink) -> {
        redisSubscriptionMessageIndex.set(state);

        final RedisMessage redisMessage = RedisMessage.builder()
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

    final List<RedisMessage> redisMessagesRoundA = redisListener.poll();

    assertTrue(redisMessagesRoundA.size() > 0);
    assertTrue(redisMessagesRoundA.size() <= 100_000);
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-0")
        .build(),
      redisMessagesRoundA.get(0));
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-" + (redisMessagesRoundA.size() - 1))
        .build(),
      redisMessagesRoundA.get(redisMessagesRoundA.size() - 1));

    // Poll again, confirming that we're getting the next batch of data
    final List<RedisMessage> redisMessagesRoundB = redisListener.poll();

    assertTrue(redisMessagesRoundB.size() > 0);
    assertTrue(redisMessagesRoundB.size() <= 100_000);
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-" + redisMessagesRoundA.size())
        .build(),
      redisMessagesRoundB.get(0));
    assertEquals(
      RedisMessage.builder()
        .channel("election")
        .message("vote-" + (redisMessagesRoundA.size() + redisMessagesRoundB.size() - 1))
        .build(),
      redisMessagesRoundB.get(redisMessagesRoundB.size() - 1));
  }
}
