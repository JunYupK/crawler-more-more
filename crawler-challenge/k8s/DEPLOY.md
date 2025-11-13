# Kubernetes Deployment Guide for Web Crawler

## 📋 Prerequisites

- Kubernetes cluster (v1.24+)
- kubectl configured
- Docker for building images
- (Optional) KEDA installed for advanced autoscaling
- (Optional) Helm for easier deployment

## 🏗️ Architecture Overview

```
┌────────────────────────────────────────────┐
│         Kubernetes Cluster                 │
│                                            │
│  ┌──────────────────────────────────────┐ │
│  │   Crawler Worker Deployment          │ │
│  │   (Auto-scaled: 1-20 pods)           │ │
│  └────────────┬─────────────────────────┘ │
│               │                            │
│  ┌────────────▼───────┐  ┌──────────────┐ │
│  │ Redis StatefulSet  │  │ PostgreSQL   │ │
│  │ (Queue Management) │  │ StatefulSet  │ │
│  └────────────────────┘  └──────────────┘ │
└────────────────────────────────────────────┘
```

## 🚀 Quick Start

### 1. Build Docker Image

```bash
cd crawler-challenge

# Build the image
docker build -t crawler-worker:latest .

# (Optional) Tag for registry
docker tag crawler-worker:latest your-registry/crawler-worker:latest
docker push your-registry/crawler-worker:latest
```

### 2. Deploy to Kubernetes

```bash
# Create namespace and apply all manifests
kubectl apply -f k8s/base/namespace.yaml
kubectl apply -f k8s/base/secret.yaml
kubectl apply -f k8s/base/configmap.yaml
kubectl apply -f k8s/base/postgres-statefulset.yaml
kubectl apply -f k8s/base/redis-statefulset.yaml

# Wait for databases to be ready
kubectl wait --for=condition=ready pod -l app=postgres -n crawler --timeout=300s
kubectl wait --for=condition=ready pod -l app=redis -n crawler --timeout=300s

# Deploy crawler workers
kubectl apply -f k8s/base/crawler-deployment.yaml
```

### 3. Verify Deployment

```bash
# Check all resources
kubectl get all -n crawler

# Check pods
kubectl get pods -n crawler -w

# Check logs
kubectl logs -n crawler -l app=crawler-worker --tail=100 -f
```

## 📊 Autoscaling

### Option A: KEDA (Recommended)

Install KEDA first:
```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

Deploy KEDA ScaledObject:
```bash
kubectl apply -f k8s/autoscaling/keda-scaledobject.yaml
```

### Option B: Standard HPA

```bash
# HPA is included in keda-scaledobject.yaml
# It will activate if KEDA is not available
kubectl get hpa -n crawler
```

## 🔧 Configuration

### Update Crawler Settings

Edit `k8s/base/configmap.yaml` and apply:
```bash
kubectl apply -f k8s/base/configmap.yaml
kubectl rollout restart deployment/crawler-worker -n crawler
```

### Scale Manually

```bash
# Scale to specific replica count
kubectl scale deployment/crawler-worker --replicas=5 -n crawler

# Check current scale
kubectl get deployment crawler-worker -n crawler
```

## 📈 Monitoring

### View Metrics

```bash
# Pod resource usage
kubectl top pods -n crawler

# HPA status
kubectl get hpa -n crawler

# KEDA ScaledObject status
kubectl get scaledobject -n crawler
```

### Check Queue Length

```bash
# Connect to Redis
kubectl exec -it redis-0 -n crawler -- redis-cli

# In Redis CLI:
LLEN queue_priority_high
LLEN queue_priority_medium
LLEN queue_priority_normal
```

### Database Access

```bash
# Connect to PostgreSQL
kubectl exec -it postgres-0 -n crawler -- psql -U postgres -d crawler_db

# Sample queries:
SELECT COUNT(*) FROM crawled_pages;
SELECT domain, COUNT(*) FROM crawled_pages GROUP BY domain ORDER BY COUNT DESC LIMIT 10;
```

## 🐛 Troubleshooting

### Pods not starting

```bash
# Check events
kubectl describe pod <pod-name> -n crawler

# Check logs
kubectl logs <pod-name> -n crawler

# Check resource limits
kubectl top pods -n crawler
```

### Database connection issues

```bash
# Test PostgreSQL connectivity
kubectl run -it --rm debug --image=postgres:16-alpine --restart=Never -n crawler -- \
  psql -h postgres-service -U postgres -d crawler_db

# Test Redis connectivity
kubectl run -it --rm debug --image=redis:7-alpine --restart=Never -n crawler -- \
  redis-cli -h redis-service ping
```

### Image pull errors

```bash
# If using local image, make sure it's available on all nodes
# For minikube:
eval $(minikube docker-env)
docker build -t crawler-worker:latest .

# For kind:
kind load docker-image crawler-worker:latest --name <cluster-name>
```

## 🔒 Security Best Practices

1. **Use Secrets for sensitive data**
   ```bash
   # Update password
   kubectl create secret generic crawler-secrets \
     --from-literal=POSTGRES_PASSWORD='your-secure-password' \
     --namespace=crawler \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

2. **Enable Network Policies**
   ```bash
   # Apply network policies (to be created)
   kubectl apply -f k8s/base/network-policies.yaml
   ```

3. **Resource Quotas**
   ```bash
   # Set namespace resource limits
   kubectl apply -f k8s/base/resource-quota.yaml
   ```

## 📦 Storage Management

### Persistent Volume Claims

```bash
# Check PVCs
kubectl get pvc -n crawler

# Increase storage (if supported by storage class)
kubectl edit pvc postgres-data-postgres-0 -n crawler
```

### Backup Database

```bash
# Create backup
kubectl exec postgres-0 -n crawler -- \
  pg_dump -U postgres crawler_db > backup.sql

# Restore from backup
kubectl exec -i postgres-0 -n crawler -- \
  psql -U postgres crawler_db < backup.sql
```

## 🧹 Cleanup

```bash
# Delete all resources
kubectl delete namespace crawler

# Or delete specific components
kubectl delete -f k8s/base/crawler-deployment.yaml
kubectl delete -f k8s/base/redis-statefulset.yaml
kubectl delete -f k8s/base/postgres-statefulset.yaml
```

## 🎯 Performance Tuning

### Adjust Worker Resources

Edit `k8s/base/crawler-deployment.yaml`:
```yaml
resources:
  requests:
    cpu: "4"      # Increase for more performance
    memory: "8Gi"
  limits:
    cpu: "8"
    memory: "16Gi"
```

### Optimize Database

```bash
# Connect to PostgreSQL
kubectl exec -it postgres-0 -n crawler -- psql -U postgres -d crawler_db

# Run VACUUM
VACUUM ANALYZE crawled_pages;

# Check table size
SELECT pg_size_pretty(pg_total_relation_size('crawled_pages'));
```

## 📚 Additional Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [KEDA Documentation](https://keda.sh/docs/)
- [PostgreSQL on Kubernetes](https://www.postgresql.org/docs/)
- [Redis on Kubernetes](https://redis.io/docs/management/scaling/)

