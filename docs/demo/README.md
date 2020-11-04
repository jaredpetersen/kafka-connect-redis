# Demo
Let's run Kafka Connect Redis in a local Apache Kafka + Redis Kubernetes cluster via minikube so that we can get a feel for how it all works together.

## Setup
### Minikube
Let's set up the cluster. We're going to use minikube for this so [make sure you have it installed](https://minikube.sigs.k8s.io/docs/start/) along with [`kubectl` 1.14 or higher](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

Set up the cluster with a docker registry and some extra juice:
```bash
minikube start --cpus 2 --memory 10g
```

### Docker
Now that we have a local Kubernetes setup, we'll need a Docker image that contains Kafka Connect Redis. We don't publish a Docker image to public Docker registries since you will usually install multiple Kafka Connectors on one image.

Navigate to `demo/docker/` in this repository and run the following commands **in a separate terminal** to download the plugin and build the image for minikube:
```bash
curl -O https://search.maven.org/remotecontent?filepath=io/github/jaredpetersen/kafka-connect-redis/1.0.0/kafka-connect-redis-1.0.0.jar
eval $(minikube docker-env)
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

Run the following command to configure Redis to run in cluster mode instead of standalone mode:
```bash
kubectl -n kcr-demo run -it --rm redis-client --image redis:6 -- redis-cli --pass IEPfIr0eLF7UsfwrIlzy80yUaBG258j9 --cluster create $(kubectl -n kcr-demo get pods -l app=redis-cluster -o jsonpath='{range.items[*]}{.status.podIP}:6379 ') --cluster-yes
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