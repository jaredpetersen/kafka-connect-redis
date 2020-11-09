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

## Release Process
Only [@jaredpetersen](https://github.com/jaredpetersen) is able to deploy a new release of Kafka Connect Redis. Always deploy a new version to Maven Central and Confluent Hub.

### GitHub
1. Revise Kafka Connect Redis' version in your pull request before merging.
2. Merge your pull request to master.
3. GitHub CI will automatically build the release, publish to Maven Central staging, and create a new GitHub Release

### Maven Central
1. Log in to the [repository manager](https://oss.sonatype.org/) for Maven Central.
2. Navigate to [Staging Repositories](https://oss.sonatype.org/#stagingRepositories) and locate the staged release in the listings.
3. Click on the staged release and navigate to the Content tab.
4. Confirm that the content is correct. As an additional measure, you can download the published jar and confirm that the JAR works in the [local development cluster](/docs/development).
5. If the content is correct, close the release by clicking the Close button at the top.
6. The repository manager will begin evaluating the releasability of the plugin against the [Maven Central requirements](https://central.sonatype.org/pages/requirements.html). Assuming that everything is good to go, release the plugin by clicking the Release button at the top.
7. Drop the staged repository by clicking the Drop button at the top.
8. Wait at least an hour and [confirm that the plugin has been updated](https://search.maven.org/search?q=g:io.github.jaredpetersen%20AND%20a:kafka-connect-redis&core=gav).

### Confluent Hub
1. Find the latest GitHub Release and get the link to the Confluent Package (e.g. https://github.com/jaredpetersen/kafka-connect-redis/releases/download/1.X.X/jaredpetersen-kafka-connect-redis-1.X.X.zip).
2. Send an email to [confluent-hub@confluent.io](mailto:confluent-hub@confluent.io?Subject=Kafka%20Connect%20Redis%20Plugin%20--%20New%20Version%20Submission) with the link to the Confluent Package.
3. Wait at least 48 hours for a confirmation that the package has been uploaded.

