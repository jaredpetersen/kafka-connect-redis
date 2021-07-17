package io.github.jaredpetersen.kafkaconnectredis.source.listener.subscriber;

import io.github.jaredpetersen.kafkaconnectredis.source.listener.RedisMessage;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Abstract Redis subscriber to help facilitate internal caching of emitted messages.
 */
public abstract class RedisSubscriber {
  final ConcurrentLinkedQueue<RedisMessage> messageQueue;

  public RedisSubscriber(ConcurrentLinkedQueue<RedisMessage> messageQueue) {
    this.messageQueue = messageQueue;
  }

  /**
   * Retrieve a single cached message that was emitted to the subscriber and remove it from the cache.
   *
   * @return Cached message or null if nothing is available
   */
  public RedisMessage poll() {
    return messageQueue.poll();
  }
}
