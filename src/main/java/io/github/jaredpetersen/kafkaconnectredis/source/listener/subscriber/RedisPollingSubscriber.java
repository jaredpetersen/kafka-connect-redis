package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Common event storage mechanism for Redis subscribers.
 *
 * @param <K> Channel / pattern
 * @param <V> Message
 */
public abstract class RedisPollingSubscriber<K, V> {
  private final Queue<SubscriptionEvent<K, V>> queue = new ConcurrentLinkedQueue<>();

  private static final long MAX_POLL_SIZE = 10_000L;

  /**
   * Grab the latest subscription events in the order they were emitted.
   *
   * @return List of subscription events.
   */
  public List<SubscriptionEvent<K, V>> poll() {
    final List<SubscriptionEvent<K, V>> events = new ArrayList<>();

    while (true) {
      final SubscriptionEvent<K, V> event = this.queue.poll();

      // Subscription events may come in faster than we can iterate over them here so return early once we hit the max
      if (event == null || events.size() >= MAX_POLL_SIZE) {
        break;
      }

      events.add(event);
    }

    return events;
  }

  void recordEvent(K channel, V message) {
    final SubscriptionEvent<K, V> event = SubscriptionEvent.<K, V>builder()
      .channel(channel)
      .message(message)
      .build();

    this.queue.add(event);
  }
}
