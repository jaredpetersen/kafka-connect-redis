package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber.RedisSubscriber;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

public class RedisListener {
  private final Queue<RedisMessage> queue = new ConcurrentLinkedQueue<>();
  private final RedisSubscriber redisSubscriber;

  private Disposable listener;

  private static final long MAX_POLL_SIZE = 100_000L;

  private static final Logger LOG = LoggerFactory.getLogger(RedisListener.class);

  public RedisListener(RedisSubscriber redisSubscriber) {
    this.redisSubscriber = redisSubscriber;
  }

  /**
   * Subscribe and start listening asynchronously.
   */
  public void start() {
    this.redisSubscriber
      .subscribe()
      .doOnSuccess(empty -> LOG.info("subscribed to channel(s)"))
      .block();
    this.listener = this.redisSubscriber.observe()
      .doOnNext(redisSubscriptionMessage -> LOG.info("observed " + redisSubscriptionMessage))
      .doOnNext(this.queue::add)
      .subscribeOn(Schedulers.boundedElastic())
      .subscribe();
  }

  /**
   * Unsubscribe and stop listening.
   */
  public void stop() {
    this.redisSubscriber.unsubscribe().block();
    this.listener.dispose();
  }

  /**
   * Retrieve messages from Redis Pub/Sub.
   *
   * @return List of subscription messages.
   */
  public List<RedisMessage> poll() {
    final List<RedisMessage> redisMessages = new ArrayList<>();

    while (true) {
      final RedisMessage redisMessage = this.queue.poll();

      // No more events left, stop iterating
      if (redisMessage == null) {
        break;
      }

      redisMessages.add(redisMessage);

      // Subscription events may come in faster than we can iterate over them here so return early once we hit the max
      if (redisMessages.size() >= MAX_POLL_SIZE) {
        break;
      }
    }

    return redisMessages;
  }
}
