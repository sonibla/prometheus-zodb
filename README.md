# prometheus-zodb

Prometheus collector for [ZODB](https://zodb.org/) database metrics.

Works with any ZODB storage backend (FileStorage, RelStorage, MemoryStorage, etc.).

## Metrics exposed

| Metric | Type | Description |
|---|---|---|
| `zodb_connection_pool_size` | Gauge | Current number of open connections in the pool |
| `zodb_cache_size` | Gauge | Number of objects currently in the connection cache (per connection) |
| `zodb_cache_non_ghost_count` | Gauge | Number of non-ghost (loaded) objects in the cache (per connection) |
| `zodb_activity_loads` | Gauge | Object loads recorded by the activity monitor |
| `zodb_activity_stores` | Gauge | Object stores recorded by the activity monitor |
| `zodb_activity_connections` | Gauge | Connections opened recorded by the activity monitor |

## Usage

```python
from ZODB import DB
from prometheus_zodb import ZODBCollector
from prometheus_client import REGISTRY

db = DB(storage)

# Register on the default global registry (picked up by start_http_server)
ZODBCollector(db)

# Or use a custom registry
from prometheus_client import CollectorRegistry
registry = CollectorRegistry()
ZODBCollector(db, registry=registry)

# Or manage registration manually
collector = ZODBCollector(db, registry=None)
registry.register(collector)
```
