# Prometheus & Grafana Monitoring Stack

## 📊 Overview

Complete monitoring solution for the Crawler system with:
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **PostgreSQL Exporter**: Database metrics
- **Redis Exporter**: Queue metrics

## 🏗️ Architecture

```
┌────────────────────────────────────────┐
│        Grafana (Port 3000)             │
│     Visualization Dashboard            │
└───────────────┬────────────────────────┘
                │
┌───────────────▼────────────────────────┐
│      Prometheus (Port 9090)            │
│     Metrics Collection & Storage       │
└───┬───────┬───────┬────────────────────┘
    │       │       │
    ▼       ▼       ▼
┌────────┐ ┌────────┐ ┌──────────────┐
│Crawler │ │Postgres│ │    Redis     │
│Workers │ │Exporter│ │  Exporter    │
└────────┘ └────────┘ └──────────────┘
```

## 🚀 Quick Deploy

### Prerequisites
- Kubernetes cluster running
- Crawler namespace created
- Base crawler stack deployed

### 1. Deploy Monitoring Stack

```bash
cd crawler-challenge/k8s/monitoring

# Deploy Prometheus
kubectl apply -f prometheus/prometheus-configmap.yaml
kubectl apply -f prometheus/prometheus-deployment.yaml

# Deploy Grafana
kubectl apply -f grafana/grafana-configmap.yaml
kubectl apply -f grafana/grafana-deployment.yaml
kubectl apply -f grafana/crawler-dashboard-configmap.yaml

# Deploy Exporters
kubectl apply -f exporters.yaml

# (Optional) Deploy Ingress
kubectl apply -f ingress.yaml
```

### 2. Verify Deployment

```bash
# Check all pods
kubectl get pods -n crawler

# Expected output:
# prometheus-xxx          1/1     Running
# grafana-xxx             1/1     Running
# postgres-exporter-xxx   1/1     Running
# redis-exporter-xxx      1/1     Running
```

### 3. Access Dashboards

#### Grafana (via LoadBalancer)
```bash
# Get Grafana external IP
kubectl get svc grafana -n crawler

# Access: http://<EXTERNAL-IP>:3000
# Default credentials: admin / admin
```

#### Grafana (via Port-Forward)
```bash
kubectl port-forward -n crawler svc/grafana 3000:3000

# Access: http://localhost:3000
```

#### Prometheus (via Port-Forward)
```bash
kubectl port-forward -n crawler svc/prometheus 9090:9090

# Access: http://localhost:9090
```

## 📈 Dashboards

### Crawler Performance Dashboard

Pre-configured dashboard includes:

1. **Overview Metrics**
   - Running Worker Pods
   - Total CPU Usage
   - Total Memory Usage
   - Redis Queue Length

2. **Pod Metrics**
   - CPU Usage per Pod
   - Memory Usage per Pod
   - Pod Restarts

3. **Queue Metrics**
   - Queue Lengths by Priority
   - Queue Processing Rate

4. **Database Metrics**
   - PostgreSQL Connections
   - Database Size
   - Transaction Rate

5. **Network Metrics**
   - Network Traffic (RX/TX)
   - Bandwidth Usage

### Accessing the Dashboard

1. Login to Grafana (admin/admin)
2. Go to **Dashboards** → **Browse**
3. Select **"Crawler Performance Dashboard"**

## 🔍 Key Metrics

### Prometheus Queries

**Worker Pod Count:**
```promql
count(kube_pod_status_phase{namespace="crawler", pod=~"crawler-worker.*", phase="Running"})
```

**Total Queue Length:**
```promql
redis_list_length{list="queue_priority_high"} + 
redis_list_length{list="queue_priority_medium"} + 
redis_list_length{list="queue_priority_normal"}
```

**CPU Usage:**
```promql
sum(rate(container_cpu_usage_seconds_total{namespace="crawler", pod=~"crawler-worker.*"}[5m]))
```

**Memory Usage:**
```promql
sum(container_memory_working_set_bytes{namespace="crawler", pod=~"crawler-worker.*"}) / 1024 / 1024 / 1024
```

**Database Connections:**
```promql
pg_stat_activity_count
```

## ⚙️ Configuration

### Prometheus

**Data Retention:**
```yaml
# In prometheus-deployment.yaml
args:
  - '--storage.tsdb.retention.time=15d'  # Keep metrics for 15 days
```

**Scrape Interval:**
```yaml
# In prometheus-configmap.yaml
global:
  scrape_interval: 15s  # Scrape every 15 seconds
```

### Grafana

**Admin Password:**
```yaml
# In grafana-deployment.yaml
env:
  - name: GF_SECURITY_ADMIN_PASSWORD
    value: "your-secure-password"  # Change this!
```

**Plugins:**
```yaml
env:
  - name: GF_INSTALL_PLUGINS
    value: "grafana-piechart-panel,grafana-clock-panel"
```

## 📊 Custom Dashboards

### Creating New Dashboard

1. Login to Grafana
2. Click **+** → **Dashboard** → **Add new panel**
3. Write PromQL query
4. Configure visualization
5. Save dashboard

### Importing Dashboards

```bash
# Via ConfigMap
kubectl create configmap my-dashboard \
  --from-file=my-dashboard.json \
  --namespace=crawler

# Update grafana-deployment.yaml to mount the ConfigMap
```

## 🔔 Alerting (Optional)

### Adding Alert Rules

Edit `prometheus-configmap.yaml`:

```yaml
rule_files:
  - /etc/prometheus/alerts.yml

# Add alert rules
groups:
  - name: crawler_alerts
    rules:
      - alert: HighQueueLength
        expr: redis_list_length > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High queue length detected"
          
      - alert: HighMemoryUsage
        expr: container_memory_working_set_bytes > 7516192768  # 7GB
        for: 10m
        labels:
          severity: critical
```

### Deploy Alertmanager

```bash
# Create alertmanager deployment
kubectl apply -f alertmanager.yaml

# Update prometheus config to send alerts
```

## 🐛 Troubleshooting

### Prometheus not scraping

```bash
# Check Prometheus targets
kubectl port-forward -n crawler svc/prometheus 9090:9090
# Visit: http://localhost:9090/targets

# Check pod annotations
kubectl describe pod <crawler-worker-pod> -n crawler | grep annotations
```

### Grafana datasource not working

```bash
# Check Prometheus service
kubectl get svc prometheus -n crawler

# Test from Grafana pod
kubectl exec -it <grafana-pod> -n crawler -- \
  wget -qO- http://prometheus:9090/-/healthy
```

### Missing metrics

```bash
# Check exporter logs
kubectl logs -n crawler <postgres-exporter-pod>
kubectl logs -n crawler <redis-exporter-pod>

# Test exporter endpoints
kubectl port-forward -n crawler svc/postgres-exporter 9187:9187
curl http://localhost:9187/metrics
```

### Dashboard not showing

```bash
# Check ConfigMap
kubectl get cm grafana-crawler-dashboard -n crawler

# Check Grafana logs
kubectl logs -n crawler <grafana-pod>

# Restart Grafana
kubectl rollout restart deployment/grafana -n crawler
```

## 📦 Storage Management

### Increase Prometheus Storage

```bash
# Edit PVC
kubectl edit pvc prometheus-pvc -n crawler

# Increase size
spec:
  resources:
    requests:
      storage: 20Gi  # Increase from 10Gi
```

### Backup Prometheus Data

```bash
# Snapshot API
curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot

# Or use velero for full backup
velero backup create prometheus-backup \
  --include-namespaces crawler \
  --include-resources pvc,pv
```

## 🔐 Security Best Practices

1. **Change default passwords**
```bash
kubectl create secret generic grafana-admin \
  --from-literal=username=admin \
  --from-literal=password='<strong-password>' \
  --namespace=crawler
```

2. **Enable HTTPS** (via Ingress + cert-manager)
```yaml
# Add to ingress.yaml
annotations:
  cert-manager.io/cluster-issuer: letsencrypt-prod
tls:
  - hosts:
      - grafana.yourdomain.com
    secretName: grafana-tls
```

3. **Restrict access** (via NetworkPolicy)
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: grafana-access
  namespace: crawler
spec:
  podSelector:
    matchLabels:
      app: grafana
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: ingress-nginx
```

## 📚 Additional Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Exporters](https://prometheus.io/docs/instrumenting/exporters/)
- [PromQL Cheat Sheet](https://promlabs.com/promql-cheat-sheet/)

