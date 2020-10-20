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

## Usage
### Create Kafka Topics
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-create-topics --image confluentinc/cp-kafka:5.4.3 --command /bin/bash
```

Create topic:
```
kafka-topics --create --zookeeper zookeeper-0.zookeeper:2181 --replication-factor 1 --partitions 1 --topic rediscommands
```

### Configure Kafka Connect Redis
Send a request to the Kafka Connect REST API to configure it to use Kafka Connect Redis:
```bash
curl --request POST \
    --url "$(minikube -n kcr-demo service kafka-connect --url)/connectors" \
    --header 'content-type: application/json' \
    --data '{
        "name": "demo-redis-connector2",
        "config": {
            "connector.class": "io.github.jaredpetersen.kafkaconnectredis.sink.RedisSinkConnector",
            "tasks.max": "1",
            "topics": "rediscommands2",
            "redis.uri": "redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster"
        }
    }'
```

### Write Records
Create an interactive ephemeral query pod:
```bash
kubectl -n kcr-demo run -it --rm kafka-write-records --image confluentinc/cp-kafka:5.4.3 --command /bin/bash
```

Write records to the `rediscommands` topic:
```bash
kafka-console-producer --broker-list kafka-broker-0.kafka-broker:9092 --topic rediscommands2
>{ "payload": { "command": "SET", "payload": { "key": "{user.1}.username", "value": "jetpackmelon22" } }, "schema": { "type": "struct", "fields": [ { "field": "command", "type": "string", "optional": false }, { "field": "payload", "type": "struct", "fields": [ { "field": "key", "type": "string", "optional": false }, { "field": "value", "type": "string", "optional": false }, { "field": "expiration", "type": "struct", "fields": [ { "field": "type", "type": "string", "optional": false }, { "field": "time", "type": "int64", "optional": false } ], "optional": true }, { "field": "condition", "type": "string", "optional": true } ], "optional": false } ], "optional": false } }
>{ "payload": { "operation": "SET", "payload": { "key": "{user.2}.username", "value": "anchorgoat74", "expiration": { "type": "EX", "time": 2100 }, "condition": "NX" } }, "schema": { "type": "struct", "fields": [ { "field": "operation", "type": "string", "optional": false }, { "field": "payload", "type": { "type": "struct", "fields": [ { "field": "key", "type": "string", "optional": false }, { "field": "value", "type": "string", "optional": false }, { "field": "expiration", "type": "struct", "fields": [ { "field": "type", "type": "string", "optional": false }, { "field": "time", "type": "int64", "optional": false } ], "optional": true }, { "field": "condition", "type": "string", "optional": true } ], "optional": false } } ] } }
```

### Redis
Create Redis client pod:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- /bin/bash
```

Use redis-cli to connect to the cluster:
```bash
redis-cli -c -u 'redis://IEPfIr0eLF7UsfwrIlzy80yUaBG258j9@redis-cluster'
```

### Validate
Open up the `demo` database again and go to the collections we created earlier. You should now see that they have the data we just wrote into Kafka.

## Teardown
Remove all manifests:
```bash
k delete -k kubernetes
```

Delete the minikube cluster
```bash
minikube delete
```
