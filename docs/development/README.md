# Development
## Useful Commands
```bash
mvn clean install
mvn clean compile
mvn clean test
mvn clean verify
mvn clean package
```

## Development Cluster
See [Demo](../demo) for how to run Kafka Connect Redis in a local minikube cluster. This can be useful for manual end-to-end testing.

To use local code instead of one of the releases, run `mvn clean package` and copy the resulting JAR to `docs/demo/docker/` instead of downloading one of the releases.
