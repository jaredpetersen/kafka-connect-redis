# Demo
Let's run Kafka Connect Redis in a local Apache Kafka + Redis Kubernetes cluster via MiniKube so that we can get a feel for how it all works together.

## Setup
### Minikube
Let's set up the cluster. We're going to use Minikube for this so [make sure you have it installed](https://minikube.sigs.k8s.io/docs/start/) along with [`kubectl` 1.14 or higher](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

Set up the cluster with a docker registry and some extra juice:
```bash
minikube start --cpus 2 --memory 10g
```

### Docker
Now that we have a cluster, we'll need a Docker image that contains the Kafka Connect Redis plugin. We don't publish a Docker image to public Docker registries since you will usually install multiple Kafka Connect plugins on one image. Additionally, that base image may vary depending on your preferences and use case.

Navigate to `demo/docker/` and run the following commands in a separate terminal to download the plugin and build the image for Minikube:
```bash
curl -O https://search.maven.org/remotecontent?filepath=io/github/jaredpetersen/kafka-connect-redis/1.0.0/kafka-connect-redis-1.0.0.jar
eval $(minikube docker-env)
docker build -t jaredpetersen/kafka-connect-redis:latest .
```

Alternatively, obtain the JAR file by building from source with `mvn package` at the root of this repository.

Close out this terminal when you're done -- we want to go back to our normal Docker environment.

### Kubernetes Manifests
Let's start running everything. Apply the manifests (you may have to give this a couple of tries due to race conditions):
```bash
kubectl apply -k kubernetes
```

Check in on the pods and wait for everything to come up:
```bash
kubectl -n kcr-demo get pods
```

Be patient, this can take a few minutes.

Run the following command to configure redis to run in cluster mode instead of standalone mode:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- redis-cli --pass IEPfIr0eLF7UsfwrIlzy80yUaBG258j9 --cluster create $(kubectl -n kcr-demo get pods -l app=redis-cluster -o jsonpath='{range.items[*]}{.status.podIP}:6379 ') --cluster-yes
```

Run the following command to create the topic the sink connector will listen to. Kafka can automatically create topics, but we need to make sure that the sink topic can support multiple schemas in the same topic.
```bash
kubectl -n kcr-demo run -it --rm kafka-create-topic --image confluentinc/cp-kafka:5.4.3 -- kafka-topics --create --bootstrap-server 'kafka-broker-0.kafka-broker:9092' --config 'confluent.value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy' --topic 'redis.commands'
```

## Usage
[Source Connector](SOURCE.md)

[Sink Connector](SINK.md)

## Teardown
Remove all of the created resources in Kubernetes:
```bash
k delete -k kubernetes
```

Delete the minikube cluster:
```bash
minikube delete
```