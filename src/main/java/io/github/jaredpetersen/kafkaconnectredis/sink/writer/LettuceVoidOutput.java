package io.github.jaredpetersen.kafkaconnectredis.sink.writer;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import java.nio.ByteBuffer;

/**
 * Void output of a Redis command used to "fire and forget".
 * <p />
 * Temporary stopgap until https://github.com/lettuce-io/lettuce-core/issues/1529 is officially supported.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
class LettuceVoidOutput<K, V> extends CommandOutput<K, V, Void> {
  public LettuceVoidOutput(RedisCodec<K, V> codec) {
    super(codec, null);
  }

  @Override
  public void set(ByteBuffer bytes) {
  }

  @Override
  public void set(long integer) {
  }

  @Override
  public void set(double number) {
  }

  @Override
  public void set(boolean value) {
  }
}
