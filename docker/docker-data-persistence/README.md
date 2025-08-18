# Docker Data Persistance

## Build Image

```bash
docker build -t docker-data-persistance-app .
```

## Create the volumen

```bash
docker volume create app-logs
```

## Run container with volumens 

```bash
docker run -d \
  --name web-app-with-all-mounts \
  --mount source=app-logs,target=/app/logs \
  --mount type=bind,source=$(pwd)/app-config,target=/app/config \
  -p 8010:8010 \
  docker-data-persistance-app
```

## Run container without volumens 

```bash
docker run -d \
  --name web-app-without-mounts \
  -p 8011:8010 \
  docker-data-persistance-app
```

## Delete Container 

```bash
docker stop web-app-with-all-mounts web-app-without-mounts
docker rm web-app-with-all-mounts web-app-without-mounts
```

## Delete image

```bash
docker rmi docker-data-persistance-app
```

## List volumen

```bash
docker volume ls
```

## Delete volumen

```bash
docker volume rm app-logs
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

