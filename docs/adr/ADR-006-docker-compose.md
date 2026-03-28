# ADR-006: Docker Compose over Kubernetes for Local Development

- **Status:** Accepted
- **Date:** 2026-03-25
- **Author:** Khairunnisa Maharani

---

## Context

GlowCart requires multiple services to run simultaneously: Apache Kafka, Apache Airflow, FastAPI, and supporting dependencies. An orchestration tool is needed to manage these containers consistently across development environments.

Two realistic options were evaluated: Docker Compose (single-host container orchestration) and Kubernetes (distributed container orchestration, typically via minikube or kind for local use).

The goal at this stage is a reproducible local environment that any reviewer can spin up with a single command — not a production-grade deployment.

---

## Decision

Use **Docker Compose** as the container orchestration layer for all local development and demonstration.

All services are defined in a single `docker-compose.yml` at the project root. The full stack starts with `docker compose up -d`.

---

## Consequences

**Positive:**
- Zero setup friction — Docker Desktop is the only prerequisite.
- Single command to start the entire stack (`docker compose up -d`).
- Easier to read and modify than Kubernetes manifests; reviewers can understand the infrastructure in minutes.
- Sufficient for demonstrating pipeline correctness, idempotency, and data quality — the actual goals of this project.

**Negative / Trade-offs:**
- Cannot simulate horizontal scaling. Adding more Kafka consumer replicas is not meaningful in a single-host environment.
- No self-healing — if a container crashes, it must be restarted manually (or via `restart: always` policy, which is a blunt instrument).
- Not representative of how this pipeline would actually be deployed in production.

---

## Alternatives Considered

**Kubernetes (minikube / kind)**
Would allow simulating horizontal scaling, pod autoscaling, and rolling deployments. However, the setup complexity is significantly higher — requires installing minikube, writing Helm charts or raw manifests, and managing namespaces and resource limits. For a portfolio project focused on data engineering correctness (not infra), this overhead is not justified.

**No orchestration (manual `docker run`)**
Running containers manually makes the setup non-reproducible and difficult for reviewers to replicate. Rejected immediately.

---

## Migration Path (at scale)

In a production environment, this pipeline would migrate to Kubernetes:

- Each pipeline component (Kafka consumer, Airflow worker, FastAPI) becomes a separate `Deployment` with its own replica count.
- Kafka consumers can scale horizontally by matching replica count to partition count.
- Helm charts replace the `docker-compose.yml`, enabling environment-specific configuration (dev / staging / prod).
- Health checks and liveness probes replace Docker's `restart: always`.

This migration is intentionally deferred — the current Docker Compose setup is the right tool for local development and portfolio demonstration.