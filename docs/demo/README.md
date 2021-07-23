# Demo
Let's run Kafka Connect Redis against Kafka and a Redis cluster to get a feel for how it all works. We'll be running everything in Kubernetes via minikube since it's one of the easiest ways to do so without installing a ton of software on your machine.

## Setup
### Minikube
First we need to set up Kubernetes. We're going to use minikube for this so [make sure you have it installed](https://minikube.sigs.k8s.io/docs/start/) along with [`kubectl` 1.14 or higher](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

Set up minkube with some extra juice:
```bash
minikube start --cpus 2 --memory 10g
```

### Docker
Now that we have Kubernetes set up locally, we'll need some Docker images.

Open a new terminal we can use to build images for minikube. Run the following command to connect the terminal to minikube:
```bash
eval $(minikube docker-env)
```

We'll use this terminal for the rest of this section.

Let's start by building Redis. Navigate to `demo/docker/redis` and run the following commands:
```bash
docker build -t jaredpetersen/redis:latest .
```

Next, we'll need to build a docker image for Kafka Connect Redis. Navigate to `demo/docker/kafka-connect-redis` and run the following commands:
```bash
curl -O https://repo1.maven.org/maven2/io/github/jaredpetersen/kafka-connect-redis/1.2.2/kafka-connect-redis-1.2.2.jar
docker build -t jaredpetersen/kafka-connect-redis:latest .
```

Alternatively, obtain the JAR file by building from source with `mvn package` at the root of this repository.

Close out this terminal when you're done -- we want to go back to our normal Docker environment that isn't polluted by minikube.

### Kubernetes Manifests
Apply the Kubernetes manifests so that we can start running all the different services:
```bash
kubectl apply -k kubernetes
```

Check in on the pods and wait for everything to come up:
```bash
kubectl -n kcr-demo get pods
```

Be patient, this can take a few minutes.

### Redis Configuration
Run the following command to configure Redis to run in cluster mode instead of standalone mode:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- redis-cli --pass IEPfIr0eLF7UsfwrIlzy80yUaBG258j9 --cluster create $(kubectl -n kcr-demo get pods -l app=redis-cluster -o jsonpath='{range.items[*]}{.status.podIP}:6379 {end}') --cluster-yes
```

#### Add New Cluster Node (Optional)
You may find it useful to add a node to the Redis cluster later to simulate how the connector keeps up with topology changes.

To accomplish this, you need to update `kubernetes/redis/statefulset.yaml` to specify the new desired replica count and apply it with:
```bash
kubectl apply -k kubernetes
```

Next, you need to add the new node to the cluster configuration.

Find the IP address number of the new node:
```bash
kubectl -n kcr-demo get pod redis-cluster-### -o jsonpath='{.status.podIP}'
```

Find the IP address of one of the nodes already in the cluster:
```bash
kubectl -n kcr-demo get pods -l app=redis-cluster -o jsonpath='{.items[0].status.podIP}'
```

Create Redis client pod so that we can update the cluster configuration:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- /bin/bash
```

Save those two IP addresses -- and the Redis cluster password while we're at it -- as environment variables:
```bash
NEW_NODE=newnodeipaddress:6379
EXISTING_NODE=existingnodeipaddress:6379
PASSWORD=IEPfIr0eLF7UsfwrIlzy80yUaBG258j9
```

Add the node to the cluster using the IP address information you collected earlier:
```bash
redis-cli --pass $PASSWORD --cluster add-node $NEW_NODE $EXISTING_NODE
```

Connect to the cluster and confirm that there is now an additional entry in the cluster listing:
```bash
redis-cli -c -a $PASSWORD -u "redis://redis-cluster"
redis-cluster:6379> CLUSTER NODES
```

The new upstream node doesn't have any slots assigned to it. Without slots being assigned, it can't store any data. Let's fix that by rebalancing the cluster:
```bash
redis-cli --pass $PASSWORD --cluster rebalance $EXISTING_NODE --cluster-use-empty-masters
```

Then confirm that the new node has been assigned slots:
```bash
redis-cli -c -a $PASSWORD -u "redis://redis-cluster"
redis-cluster:6379> CLUSTER NODES
```

## Usage
[Source Connector](SOURCE.md)

[Sink Connector](SINK.md)

## Teardown
Remove all of the created resources in Kubernetes:
```bash
kubectl delete -k kubernetes
```

Delete the minikube cluster:
```bash
minikube delete
```
