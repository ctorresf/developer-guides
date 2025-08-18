# Kubernetes demo

## Using Docker desktop

We can run a Kubernetes server in local using Docker desktop: 
https://docs.docker.com/desktop/features/kubernetes/
https://www.docker.com/blog/how-to-set-up-a-kubernetes-cluster-on-docker-desktop/
https://docs.docker.com/guides/kube-deploy/

## Get status of kubernetes service 

```bash
kubectl get node
```

## Create a image to deploy in Kubernetes

```bash
docker build -t docker-simple-python-app ../docker-simple-python-app
```

## Deploy into Kubernetes cluster

```bash
kubectl apply -f deployment.yaml
```

## List current pods 

```bash
kubectl get pods
```

## Get all deployments

```bash
kubectl get deployments
```

## Get services

```bash
kubectl get service
```

## Delete app

```bash
kubectl delete -f deployment.yaml
```