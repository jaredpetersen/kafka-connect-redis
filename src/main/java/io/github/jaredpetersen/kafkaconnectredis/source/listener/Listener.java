package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.Mono;

public class Listener {
  private final RedisReactiveCommands<String, String> redisStandaloneCommands;
  private final RedisClusterReactiveCommands<String, String> redisClusterCommands;
  private final boolean clusterEnabled;

  /**
   * Set up listener to interact with standalone Redis and begin listening.
   *
   * @param redisStandaloneCommands Standalone Redis to listen to.
   */
  public Listener(RedisReactiveCommands<String, String> redisStandaloneCommands) {
    this.redisStandaloneCommands = redisStandaloneCommands;
    this.redisClusterCommands = null;
    this.clusterEnabled = false;
  }

  /**
   * Set up listener to interact with Redis cluster and begin listening.
   *
   * @param redisClusterCommands Redis cluster to listen to.
   */
  public Listener(RedisClusterReactiveCommands<String, String> redisClusterCommands) {
    this.redisStandaloneCommands = null;
    this.redisClusterCommands = redisClusterCommands;
    this.clusterEnabled = true;
  }

  /**
   * Retrieve all of the recently listened to items from Redis in the order that they arrived.
   *
   * @return Recently listened to items from Redis.
   */
  public Mono<SourceRecord> poll() {
    if (this.clusterEnabled) {
      this.redisClusterCommands.pubsubChannels("channel");
    }
  }
}
