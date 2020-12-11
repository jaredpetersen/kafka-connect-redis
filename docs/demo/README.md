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
curl -O https://repo1.maven.org/maven2/io/github/jaredpetersen/kafka-connect-redis/1.1.0/kafka-connect-redis-1.1.0.jar
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
kubectl delete -k kubernetes
```

Delete the minikube cluster:
```bash
minikube delete
```