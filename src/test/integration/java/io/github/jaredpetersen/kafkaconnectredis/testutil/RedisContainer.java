package io.github.jaredpetersen.kafkaconnectredis.testutil;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class RedisContainer extends GenericContainer<RedisContainer> {
  /**
   * Redis container.
   */
  public RedisContainer() {
    super(DockerImageName.parse("redis:6"));

    withExposedPorts(6379);

    waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
  }

  /**
   * Enable cluster mode.
   *
   * @return this.
   */
  public RedisContainer withClusterMode() {
    withCopyFileToContainer(MountableFile.forClasspathResource("redis/redis-cluster.conf"), "/data/redis.conf");
    withCopyFileToContainer(MountableFile.forClasspathResource("redis/nodes-cluster.conf"), "/data/nodes.conf");
    withCommand("redis-server", "/data/redis.conf");
    waitingFor(Wait.forLogMessage(".*Cluster state changed: ok*\\n", 1));

    return this;
  }

  /**
   * Get Redis URI.
   *
   * @return Redis URI.
   */
  public String getUri() {
    return "redis://" + this.getHost() + ":" + this.getFirstMappedPort();
  }
}
