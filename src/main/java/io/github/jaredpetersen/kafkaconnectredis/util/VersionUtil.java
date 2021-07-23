package io.github.jaredpetersen.kafkaconnectredis.util;

import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Provide version information.
 */
@Slf4j
public class VersionUtil {
  private static final Properties PROPERTIES = new Properties();

  static {
    try {
      PROPERTIES.load(VersionUtil.class.getClassLoader().getResourceAsStream("kafka-connect-redis.properties"));
    }
    catch (IOException exception) {
      LOG.error("Failed to load properties", exception);
    }
  }

  /**
   * Get version.
   *
   * @return package version
   */
  public static String getVersion() {
    return PROPERTIES.getProperty("version", "0.0.0");
  }
}
