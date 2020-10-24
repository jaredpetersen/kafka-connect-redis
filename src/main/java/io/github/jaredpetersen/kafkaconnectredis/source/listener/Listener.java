package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.Mono;

public class Listener {
  private final RedisReactiveCommands<String, String> redisStandaloneCommands;
  private final RedisClusterReactiveCommands<String, String> redisClusterCommands;
  private final boolean clusterEnabled;

  public Listener(RedisReactiveCommands<String, String> redisStandaloneCommands) {
    this.redisStandaloneCommands = redisStandaloneCommands;
    this.redisClusterCommands = null;
    this.clusterEnabled = false;
  }

  public Listener(RedisClusterReactiveCommands<String, String> redisClusterCommands) {
    this.redisStandaloneCommands = null;
    this.redisClusterCommands = redisClusterCommands;
    this.clusterEnabled = true;
  }

  public Mono<SourceRecord> poll() {
    if (this.clusterEnabled) {
      this.redisClusterCommands.pubsubChannels("channel")
    }
  }
}
