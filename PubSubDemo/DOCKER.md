# PubSubDemo Docker Guide

Complete guide for building and running PubSubDemo in Docker.

## Dockerfile Overview

The Dockerfile uses **multi-stage build** for optimal image size:

```dockerfile
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base    # Runtime image (smaller)
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build      # SDK for building
FROM build AS publish                                # Publish step
FROM base AS final                                   # Final runtime image
```

**Benefits:**
- ✅ Small final image size (~200MB vs ~1GB with SDK)
- ✅ Fast builds with layer caching
- ✅ Secure (no SDK tools in production image)
- ✅ Dependencies properly restored

## Building the Docker Image

### Option 1: Standalone Build

From the **root directory** of the project:

```bash
# Build the image
docker build -f PubSubDemo/Dockerfile -t pubsubdemo:latest .

# Verify the image
docker images | grep pubsubdemo
```

### Option 2: Using Docker Compose

```bash
# Build just the demo
docker-compose build pubsubdemo

# Build all services
docker-compose build
```

## Running the Container

### Standalone (without docker-compose)

#### With local broker:

```bash
# Assuming broker is running locally on port 9096
docker run --rm \
  --name pubsubdemo \
  --network host \
  -e PUBSUB_Broker__Host=localhost \
  -e PUBSUB_Broker__Port=9096 \
  -e PUBSUB_Demo__MessageInterval=1000 \
  pubsubdemo:latest
```

#### With custom network:

```bash
# Create network
docker network create pubsub-net

# Run broker
docker run -d \
  --name messagebroker \
  --network pubsub-net \
  -p 9096:9096 \
  messagebroker:latest

# Run demo
docker run --rm \
  --name pubsubdemo \
  --network pubsub-net \
  -e PUBSUB_Broker__Host=messagebroker \
  -e PUBSUB_Broker__Port=9096 \
  pubsubdemo:latest
```

### Using Docker Compose (Recommended)

The easiest way is to use the provided `compose.yaml`:

```bash
# Start everything
docker-compose up

# Start in detached mode
docker-compose up -d

# View logs
docker-compose logs -f pubsubdemo

# Stop everything
docker-compose down
```

## Configuration via Environment Variables

All configuration can be overridden with environment variables:

```bash
docker run --rm \
  -e PUBSUB_Broker__Host=192.168.1.100 \
  -e PUBSUB_Broker__Port=9096 \
  -e PUBSUB_Broker__MaxQueueSize=50000 \
  -e PUBSUB_Demo__MessageInterval=500 \
  -e PUBSUB_Demo__MessagePrefix=Docker \
  -e PUBSUB_Demo__BatchSize=20 \
  pubsubdemo:latest
```

### Available Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBSUB_Broker__Host` | 127.0.0.1 | Broker hostname/IP |
| `PUBSUB_Broker__Port` | 9096 | Broker TCP port |
| `PUBSUB_Broker__MaxQueueSize` | 10000 | Max queued messages |
| `PUBSUB_Demo__MessageInterval` | 1000 | Interval between messages (ms) |
| `PUBSUB_Demo__MessagePrefix` | Demo | Message prefix string |
| `PUBSUB_Demo__BatchSize` | 10 | Messages per batch |

## Docker Compose Configuration

The `compose.yaml` in the root directory includes the demo:

```yaml
services:
  messagebroker:
    # ... broker config ...
  
  logger:
    # ... logger config ...
  
  pubsubdemo:
    image: pubsubdemo
    build:
      context: .
      dockerfile: PubSubDemo/Dockerfile
    environment:
      - PUBSUB_Broker__Host=messagebroker
      - PUBSUB_Broker__Port=9096
      - PUBSUB_Demo__MessageInterval=1000
    networks:
      - pubsub-network
    depends_on:
      - messagebroker
    restart: unless-stopped
```

## Build Arguments

You can customize the build configuration:

```bash
# Build with Debug configuration
docker build \
  --build-arg BUILD_CONFIGURATION=Debug \
  -f PubSubDemo/Dockerfile \
  -t pubsubdemo:debug \
  .

# Build with specific SDK version
docker build \
  --build-arg SDK_VERSION=9.0.100 \
  -f PubSubDemo/Dockerfile \
  -t pubsubdemo:latest \
  .
```

## Optimizing Build Times

### Use BuildKit

Enable Docker BuildKit for faster builds:

```bash
export DOCKER_BUILDKIT=1
docker build -f PubSubDemo/Dockerfile -t pubsubdemo:latest .
```

Or permanently in daemon.json:
```json
{
  "features": {
    "buildkit": true
  }
}
```

### Leverage Build Cache

The Dockerfile is optimized to cache layers:

1. **Layer 1**: Copy only .csproj files → rarely changes
2. **Layer 2**: Restore dependencies → cached until .csproj changes
3. **Layer 3**: Copy source code → changes frequently
4. **Layer 4**: Build → only runs if source changed
5. **Layer 5**: Publish → only runs if build changed

Typical rebuild (no dependency changes): **~10 seconds**

### .dockerignore

The `.dockerignore` file excludes unnecessary files from build context:

```
**/.git
**/bin
**/obj
**/.vs
**/.vscode
**/TestResults
```

This reduces build context size from ~500MB to ~50MB!

## Troubleshooting

### Problem: "Unable to find project or directory"

**Cause**: Building from wrong directory

**Solution**: Always build from the **repository root**:
```bash
cd /path/to/PubSub  # Root directory
docker build -f PubSubDemo/Dockerfile -t pubsubdemo .
```

### Problem: "Connection refused" when running container

**Cause**: Container can't reach broker

**Solution**: 
1. Use correct network configuration
2. On Linux, use `--network host`
3. On Mac/Windows, use broker's Docker network IP or container name

### Problem: "Cannot find Publisher.csproj"

**Cause**: Build context doesn't include Publisher project

**Solution**: Build from root directory with context `.`:
```bash
docker build -f PubSubDemo/Dockerfile -t pubsubdemo:latest .
#                                                          ^ important!
```

### Problem: Large image size

**Cause**: Using wrong base image

**Solution**: Ensure final stage uses `runtime` not `sdk`:
```dockerfile
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base  # ✅ Good (~200MB)
# NOT
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS base     # ❌ Bad (~1GB)
```

### Problem: Slow builds

**Solution**: 
1. Enable BuildKit
2. Use `.dockerignore`
3. Don't clean between builds (cache is your friend)
4. Consider multi-platform builds only for releases

## Advanced Usage

### Running Multiple Instances

```bash
# Start 5 instances
for i in {1..5}; do
  docker run -d \
    --name pubsubdemo-$i \
    --network pubsub-network \
    -e PUBSUB_Broker__Host=messagebroker \
    -e PUBSUB_Demo__MessagePrefix=Demo-$i \
    pubsubdemo:latest
done

# View all logs
docker logs -f pubsubdemo-1
```

### Custom Entry Point

Override the entry point for debugging:

```bash
# Start with bash instead of running app
docker run -it --rm \
  --entrypoint /bin/bash \
  pubsubdemo:latest

# Then manually run
dotnet PubSubDemo.dll
```

### Mount Configuration File

Mount custom `appsettings.json`:

```bash
docker run --rm \
  -v $(pwd)/my-appsettings.json:/app/appsettings.json:ro \
  --network pubsub-network \
  -e PUBSUB_Broker__Host=messagebroker \
  pubsubdemo:latest
```

### Health Checks

Add a health check to Dockerfile:

```dockerfile
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD ps aux | grep PubSubDemo || exit 1
```

### Resource Limits

Limit CPU and memory:

```bash
docker run --rm \
  --cpus=".5" \
  --memory="256m" \
  --name pubsubdemo \
  --network pubsub-network \
  -e PUBSUB_Broker__Host=messagebroker \
  pubsubdemo:latest
```

Or in docker-compose.yaml:
```yaml
pubsubdemo:
  # ... other config ...
  deploy:
    resources:
      limits:
        cpus: '0.5'
        memory: 256M
      reservations:
        cpus: '0.25'
        memory: 128M
```

## Image Information

### Inspect the Image

```bash
# View image details
docker inspect pubsubdemo:latest

# View image layers
docker history pubsubdemo:latest

# View image size
docker images pubsubdemo
```

### Scan for Vulnerabilities

```bash
# Using Docker Scout
docker scout cves pubsubdemo:latest

# Using Trivy
trivy image pubsubdemo:latest
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Build Docker Image

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Build image
        run: |
          docker build \
            -f PubSubDemo/Dockerfile \
            -t pubsubdemo:${{ github.sha }} \
            .
      
      - name: Test image
        run: |
          docker run --rm \
            -e PUBSUB_Broker__Host=test \
            pubsubdemo:${{ github.sha }} \
            --help
```

### GitLab CI Example

```yaml
build-docker:
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -f PubSubDemo/Dockerfile -t pubsubdemo:$CI_COMMIT_SHA .
    - docker push pubsubdemo:$CI_COMMIT_SHA
```

## Production Recommendations

For production deployments:

1. **Use specific tags**: `pubsubdemo:v1.2.3` not `latest`
2. **Scan for vulnerabilities** regularly
3. **Use multi-stage builds** (already implemented ✅)
4. **Set resource limits**
5. **Configure restart policies**
6. **Use secrets** for sensitive configuration
7. **Enable health checks**
8. **Monitor container metrics**

## Clean Up

```bash
# Remove demo container
docker rm -f pubsubdemo

# Remove demo image
docker rmi pubsubdemo:latest

# Remove all demo instances
docker ps -a | grep pubsubdemo | awk '{print $1}' | xargs docker rm -f

# Clean up everything (careful!)
docker-compose down --volumes --rmi all
```

## Summary

✅ **Dockerfile Location**: `PubSubDemo/Dockerfile`  
✅ **Build Command**: `docker build -f PubSubDemo/Dockerfile -t pubsubdemo .`  
✅ **Run Command**: `docker run --rm --network host -e PUBSUB_Broker__Host=localhost pubsubdemo`  
✅ **Compose Command**: `docker-compose up`  

The Docker setup is production-ready with multi-stage builds, proper caching, and flexible configuration! 🚀

