# Simple python web app with Docker

## Create the docker image

```bash
docker build -t docker-simple-python-app .
```

# Run the container

```bash
docker run -d -p 8080:8000 --name docker-simple-python-app-container1 docker-simple-python-app
```

## List containers

```bash
docker ps 
```

## Create a new container using the previous image

```bash
docker run -d -p 8081:8000 --name docker-simple-python-app-container2 docker-simple-python-app
```

## List all containers

```bash
docker ps -a 
```

## Remove a container 

### Stop a specific container

```bash
docker stop docker-simple-python-app-container2
```

### Remove a specific container

```bash
docker rm docker-simple-python-app-container2
```

Alternatively, to force removal of a running container without stopping it first:

```bash
docker rm -f docker-simple-python-app-container1
```

Removing all stopped containers:

```bash
docker rm $(docker ps -a -q)
```

Removing all unused containers (including stopped ones and dangling volumes):

```bash
docker container prune
```

## List images

```bash
docker images
```
Also, we can run 

```bash
docker images ls
```

## Delete a image

```bash
docker rmi docker-simple-python-app
```

--- 
# Docker hub (hub.docker.com)

upload a image to docker hub 

```bash
docker tag docker-simple-python-app ctorresf/developer-guides:first_app
docker push ctorresf/developer-guides:first_app
```

eliminar app 

```bash
docker rmi ctorresf/developer-guides:first_app
```

## get a image from Docker hub

```bash
docker pull bitnami/jupyter-base-notebook:latest
```

# Run image from Docker hub

```bash
docker run -d -p 8080:8000 --name dockerfile-python-app-container1 ctorresf/developer-guides:first_app
```

# Download official prebuild images from Docker hub

https://hub.docker.com/r/yeasy/simple-web

Run container with the --rm flag to delete container after exit and -it to maintain a interactive shell with the container

```bash
docker run --rm -it -p 80:80 yeasy/simple-web:latest
```

A bigger publisher of official images is https://hub.docker.com/u/bitnami