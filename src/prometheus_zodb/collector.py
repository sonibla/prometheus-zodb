from typing import Generator

from prometheus_client import CollectorRegistry, REGISTRY
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from ZODB import DB
from ZODB.Connection import Connection


class CumulativeActivityMonitor:
    """ZODB activity monitor that accumulates totals since instantiation.

    Unlike the built-in :class:`ZODB.ActivityMonitor.ActivityMonitor`, this
    monitor keeps a simple running total instead of a fixed-length sliding
    window.  This makes it suitable for use with Prometheus, which expects
    cumulative counters (monotonically increasing since process start).

    ZODB calls :meth:`closedConnection` each time a connection is closed.
    No other interface is required.

    Usage::

        from prometheus_zodb import CumulativeActivityMonitor

        db.setActivityMonitor(CumulativeActivityMonitor())
    """

    def __init__(self) -> None:
        self.loads: int = 0
        self.stores: int = 0
        self.connections: int = 0

    def closedConnection(self, conn: Connection) -> None:
        loads, stores = conn.getTransferCounts(clear=True)
        self.loads += loads
        self.stores += stores
        self.connections += 1


class ZODBCollector:
    """Prometheus collector for ZODB database metrics.

    Collects connection pool depth, per-connection object cache sizes, and
    optionally cumulative activity counters (loads, stores, connections).

    Works with any ZODB storage backend (FileStorage, RelStorage,
    MemoryStorage, etc.).

    Activity metrics
    ----------------
    By default (``install_activity_monitor=True``), a
    :class:`CumulativeActivityMonitor` is installed on the database if no
    activity monitor is already set.  This produces three Prometheus counters:
    ``zodb_loads_total``, ``zodb_stores_total``, and
    ``zodb_connections_total``.

    Pass ``install_activity_monitor=False`` to disable this behaviour, or
    install your own monitor before creating the collector.

    Usage::

        from prometheus_zodb import ZODBCollector

        # Register on the default global registry (installs activity monitor)
        ZODBCollector(db)

        # Use a custom registry
        ZODBCollector(db, registry=my_registry)

        # Skip auto-registration
        collector = ZODBCollector(db, registry=None)
        my_registry.register(collector)

        # Opt out of activity monitor installation
        ZODBCollector(db, install_activity_monitor=False)
    """

    def __init__(
        self,
        db: DB,
        registry: CollectorRegistry | None = REGISTRY,
        install_activity_monitor: bool = True,
    ):
        self._db = db
        if install_activity_monitor and db.getActivityMonitor() is None:
            db.setActivityMonitor(CumulativeActivityMonitor())
        if registry is not None:
            registry.register(self)

    def collect(self) -> Generator:
        pool_count = GaugeMetricFamily(
            "zodb_connection_pool_count",
            "Current number of open connections in the ZODB connection pool",
        )
        pool_size = GaugeMetricFamily(
            "zodb_connection_pool_size",
            "Maximum number of connections in the ZODB connection pool (pool size)",
        )
        pool_available = GaugeMetricFamily(
            "zodb_connection_pool_available",
            "Number of available connection slots in the ZODB connection pool",
        )
        cache_size = GaugeMetricFamily(
            "zodb_cache_size",
            "Total number of objects across all ZODB connection caches",
        )
        cache_non_ghost_count = GaugeMetricFamily(
            "zodb_cache_non_ghost_count",
            "Total number of non-ghost (fully loaded) objects across all ZODB connection caches",
        )

        connections: list[Connection] = list(self._db.pool)
        pool_count_value = len(connections)
        pool_size_value = self._db.getPoolSize()

        pool_count.add_metric([], float(pool_count_value))
        pool_size.add_metric([], float(pool_size_value))
        pool_available.add_metric(
            [],
            float(
                pool_size_value -
                pool_count_value))
        cache_size.add_metric(
            [], float(sum(len(conn._cache) for conn in connections))
        )
        cache_non_ghost_count.add_metric([], float(
            sum(conn._cache.cache_non_ghost_count for conn in connections)))

        yield pool_count
        yield pool_size
        yield pool_available
        yield cache_size
        yield cache_non_ghost_count

        monitor = self._db.getActivityMonitor()
        if isinstance(monitor, CumulativeActivityMonitor):
            loads = CounterMetricFamily(
                "zodb_loads",
                "Cumulative number of object loads since process start",
            )
            stores = CounterMetricFamily(
                "zodb_stores",
                "Cumulative number of object stores since process start",
            )
            connections_total = CounterMetricFamily(
                "zodb_connections",
                "Cumulative number of ZODB connections closed since process start",
            )
            loads.add_metric([], float(monitor.loads))
            stores.add_metric([], float(monitor.stores))
            connections_total.add_metric([], float(monitor.connections))
            yield loads
            yield stores
            yield connections_total

        # Configuration gauges
        yield from self._collect_config_metrics()

    def _collect_config_metrics(self) -> Generator:
        cache_size_limit = GaugeMetricFamily(
            "zodb_cache_size_limit",
            "Configured maximum number of non-ghost objects per ZODB connection cache",
        )
        cache_size_bytes_limit = GaugeMetricFamily(
            "zodb_cache_size_bytes_limit",
            "Configured maximum bytes per ZODB connection cache (0 means unlimited)",
        )
        historical_pool_count = GaugeMetricFamily(
            "zodb_historical_connection_pool_count",
            "Current number of open historical (read-only point-in-time) connections",
        )
        historical_pool_size = GaugeMetricFamily(
            "zodb_historical_connection_pool_size",
            "Maximum number of historical connections in the pool",
        )
        historical_cache_size_limit = GaugeMetricFamily(
            "zodb_historical_cache_size_limit",
            "Configured maximum number of non-ghost objects per historical connection cache",
        )
        db_size_bytes = GaugeMetricFamily(
            "zodb_db_size_bytes",
            "Approximate size of the ZODB storage in bytes",
        )

        cache_size_limit.add_metric([], float(self._db.getCacheSize()))
        cache_size_bytes_limit.add_metric(
            [], float(self._db.getCacheSizeBytes()))
        historical_pool_count.add_metric(
            [], float(len(list(self._db.historical_pool)))
        )
        historical_pool_size.add_metric(
            [], float(self._db.getHistoricalPoolSize())
        )
        historical_cache_size_limit.add_metric(
            [], float(self._db.getHistoricalCacheSize())
        )

        yield cache_size_limit
        yield cache_size_bytes_limit
        yield historical_pool_count
        yield historical_pool_size
        yield historical_cache_size_limit

        try:
            db_size_bytes.add_metric([], float(self._db.getSize()))
            yield db_size_bytes
        except Exception:  # noqa: BLE001 — some storages may not implement getSize()
            pass
