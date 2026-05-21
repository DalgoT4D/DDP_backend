# Prefect Job Runner - Multi-dbt Docker Image

Docker image for running Dalgo Prefect flows in EKS with support for multiple dbt versions.

## What is this?

This image replaces the process-based dbt execution with containerized job runs in Kubernetes. It contains multiple dbt virtual environments to support different client requirements while preserving the exact directory structure from the current setup.

## How to Build

### Build Arguments

The Dockerfile supports the following build arguments:

- `PREFECT_VERSION`: Prefect version to use (default: 3.1.15)
- `DBT_VENV`: Path for dbt virtual environments (default: /home/ddp/dbt)
- `CLIENTDBT_ROOT`: Path for client dbt projects (default: /mnt/appdata/clientdbts)

### Environment-Specific Builds

```bash
# From the dbt_deps directory
cd /path/to/DDP_backend/dbt_deps

# Default build (uses Prefect 3.1.15)
docker build -f Dockerfile.prefect-job-runner \
  -t tech4dev/prefect-eks-job-runner:<tag> .

# Custom Prefect version
docker build -f Dockerfile.prefect-job-runner \
  --build-arg PREFECT_VERSION=3.2.0 \
  -t tech4dev/prefect-eks-job-runner:<tag> .

# Build with custom paths
docker build -f Dockerfile.prefect-job-runner \
  --build-arg PREFECT_VERSION=3.1.15 \
  --build-arg DBT_VENV=/dev/dbt/venv \
  --build-arg CLIENTDBT_ROOT=/dev/client/dbt \
  -t tech4dev/prefect-eks-job-runner:<tag> .
  
```

## Usage

Set this image in your EKS work pool configuration. Existing deployment parameters will work unchanged since the directory structure (`/home/ddp/dbt/`, `/mnt/appdata/clientdbts/`) is preserved.