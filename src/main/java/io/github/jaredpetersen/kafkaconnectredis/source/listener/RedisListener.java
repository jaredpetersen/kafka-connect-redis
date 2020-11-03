package io.github.jaredpetersen.kafkaconnectredis.source.listener;

import java.util.List;

public interface RedisListener {
  void start();

  void stop();

  List<RedisSubscriptionMessage> poll();
}
