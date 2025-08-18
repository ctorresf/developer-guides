# Docker Container Communication Demo

# Run Demo with docker commands

## Build containers

```bash
docker build -t my-flask-app ./backend
docker build -t my-frontend-flask-app ./frontend
```

## Run containers without a network

To consume the backend endpoint access to http://localhost:8000/api/message

To consume the frontent access to http://localhost:8080

```bash
docker run -d --name backend -p 8000:5000 my-flask-app 
docker run -d --name frontend  -p 8080:8080 --env BACKEND_HOST=localhost:8000 my-frontend-flask-app
```

## Create a Docker network

```bash
docker network create my_app_network
```

## Run containers with network

```bash
docker run -d --network my_app_network --name backend -p 8000:5000 -d my-flask-app
docker run -d --name frontend --network my_app_network -p 8080:8080 --env BACKEND_HOST=backend:5000 my-frontend-flask-app
```

## Delete network

```bash
docker network rm my_app_network
```

## Delete containers

```bash
docker stop backend frontend
docker rm backend frontend
```

## Delete images

```bash
docker rmi my-flask-app  my-frontend-flask-app
```

# Run Demo with Docker Compose

## Build and run Docker Compose

```bash
docker compose up -d
```

## Rebuild images

```bash
docker compose up --build
```


## Delete all

```bash
docker compose down
```

