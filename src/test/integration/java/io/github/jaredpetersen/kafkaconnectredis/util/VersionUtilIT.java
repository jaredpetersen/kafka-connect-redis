package io.github.jaredpetersen.kafkaconnectredis.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class VersionUtilIT {
  @Test
  public void constructorDoesNothing() {
    new VersionUtil();
  }

  @Test
  public void getVersionReturnsVersion() {
    final String version = VersionUtil.getVersion();
    assertTrue(version.matches("[0-9]+\\.[0-9]+\\.[0-9]+"));
  }
}
