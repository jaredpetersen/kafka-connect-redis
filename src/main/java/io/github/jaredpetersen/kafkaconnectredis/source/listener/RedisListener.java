package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

public class RedisListener {
  private final Queue<RedisSubscriptionMessage> queue = new ConcurrentLinkedQueue<>();
  private final RedisSubscriber redisSubscriber;

  private Disposable listener;

  private static final long MAX_POLL_SIZE = 10_000L;

  public RedisListener(RedisSubscriber redisSubscriber) {
    this.redisSubscriber = redisSubscriber;
  }

  public void start() {
    this.redisSubscriber.subscribe().block();
    this.listener = this.redisSubscriber.observe()
      .subscribeOn(Schedulers.boundedElastic())
      .subscribe();
  }

  public void stop() {
    this.redisSubscriber.unsubscribe().block();
    this.listener.dispose();
  }

  public List<RedisSubscriptionMessage> poll() {
    final List<RedisSubscriptionMessage> redisMessages = new ArrayList<>();

    while (true) {
      final RedisSubscriptionMessage redisMessage = this.queue.poll();

      // Subscription events may come in faster than we can iterate over them here so return early once we hit the max
      if (redisMessage == null || redisMessages.size() >= MAX_POLL_SIZE) {
        break;
      }

      redisMessages.add(redisMessage);
    }

    return redisMessages;
  }
}
