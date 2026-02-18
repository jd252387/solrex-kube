# reindex-job Helm Chart

Deploys the Solrex one-shot reindex Kubernetes Job with:

- `ServiceAccount`
- `Role` / `RoleBinding` for ConfigMap reads
- `ConfigMap` for reindex job properties and `request.yaml`
- `Job` that executes the reindex process

## Install

```bash
helm upgrade --install reindex-job ./deploy/helm/reindex-job -n solrex
```

## Re-run the Job

Because this is a one-shot `Job`, uninstall and install again to re-run:

```bash
helm uninstall reindex-job -n solrex
helm install reindex-job ./deploy/helm/reindex-job -n solrex
```

## Common Overrides

```bash
helm upgrade --install reindex-job ./deploy/helm/reindex-job \
  -n solrex \
  --set image.repository=solrex/reindex \
  --set image.tag=latest \
  --set reindex.request.source.cluster.baseUrl=http://solr-a:8983/solr \
  --set reindex.request.target.cluster.baseUrl=http://solr-b:8983/solr
```
