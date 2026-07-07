# Terraform README — Infra stubs (Nagy, plan Task 4)
# =====================================================

# ⚠️  COST WARNING — READ BEFORE TOUCHING THIS DIRECTORY ⚠️

> **Do NOT run `terraform apply` against any paid cloud provider.**
> Doing so would violate the project's zero-cost rule
> ([`docs/reference/cost-and-licensing.md`](../../docs/reference/cost-and-licensing.md))
> and could incur unexpected charges.

## Purpose

`infra/terraform/` contains a **manifest-only stub** that documents an
equivalent managed topology for reference and portability. It is intentionally
**inert** — no provider is configured, so `terraform apply` can only generate a
plan against a local state and cannot provision paid resources by accident.

## Files

| File | Purpose |
|---|---|
| `main.tf` | Inert stub with commented example modules + cost-warning output |
| `variables.tf` | Variables defaulted to minimal/free-tier values |
| `README.md` | This file |

## How to use (free/local path)

For a real Kubernetes cluster at zero cost, use **kind** or **minikube** with
the Helm chart in `../helm/supply-chain/`:

```bash
# Install kind (free, local Kubernetes)
kind create cluster --name scf

# Deploy the chart (manifests only, no paid cloud)
helm install scf ../helm/supply-chain \
  --set sparkWorker.replicas=1 \
  --set global.imagePullPolicy=IfNotPresent
```

## Simulating a plan (safe, no resources created)

```bash
cd infra/terraform
terraform init      # initialises with no provider
terraform validate  # syntax check only
# terraform plan will warn "no provider" and exit — that is intentional.
```

## Cloud topology (documentation only)

The commented-out modules in `main.tf` document what the equivalent managed
topology *would* look like on a self-managed Kubernetes cluster. The variable
defaults (`kafka_brokers=1`, `spark_workers=2`, `environment="local"`) reflect
the free-tier sizing used locally.
