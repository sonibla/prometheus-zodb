"""Tests for prometheus_zodb.ZODBCollector and CumulativeActivityMonitor."""

import unittest

import transaction
from prometheus_client import CollectorRegistry
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from ZODB import DB
from ZODB.MappingStorage import MappingStorage

from prometheus_zodb import CumulativeActivityMonitor, ZODBCollector


def _make_db() -> DB:
    """Return a fresh in-memory ZODB database."""
    return DB(MappingStorage())


def _metrics(collector: ZODBCollector) -> dict:
    """Collect metrics and return them as {name: [sample, ...]}."""
    return {m.name: m.samples for m in collector.collect()}


def _value(metrics: dict, name: str) -> float:
    """Return the single scalar value of a metric (no labels)."""
    samples = metrics[name]
    assert len(samples) == 1, f"expected 1 sample for {
        name!r}, got {
        len(samples)}"
    return samples[0].value


def _label_value(metrics: dict, name: str, label: str) -> float:
    """Return the value of the first sample whose first label value matches."""
    for sample in metrics[name]:
        if label in sample.labels.values():
            return sample.value
    raise KeyError(f"no sample with label {label!r} in {name!r}")


class TestPoolSize(unittest.TestCase):

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(
            self.db, registry=self.registry, install_activity_monitor=False
        )

    def tearDown(self):
        self.db.close()

    def test_pool_contains_connection_after_init(self):
        # DB.__init__ opens a connection internally to create the root object
        # and returns it to the pool.  So even before any user code runs,
        # the pool already has one connection.
        metrics = _metrics(self.collector)
        self.assertEqual(_value(metrics, "zodb_connection_pool_count"), 1.0)

    def test_pool_size_one_after_close(self):
        # After opening and closing one connection it is returned to the pool.
        tm = transaction.TransactionManager()
        conn = self.db.open(tm)
        tm.abort()
        conn.close()

        metrics = _metrics(self.collector)
        self.assertEqual(_value(metrics, "zodb_connection_pool_count"), 1.0)

    def test_pool_size_two_concurrent_connections(self):
        # Two simultaneously open connections both appear in the pool.
        tm1 = transaction.TransactionManager()
        tm2 = transaction.TransactionManager()
        conn1 = self.db.open(tm1)
        conn2 = self.db.open(tm2)

        metrics = _metrics(self.collector)
        self.assertEqual(_value(metrics, "zodb_connection_pool_count"), 2.0)

        tm1.abort()
        conn1.close()
        tm2.abort()
        conn2.close()


class TestCacheMetrics(unittest.TestCase):

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(
            self.db, registry=self.registry, install_activity_monitor=False
        )

    def tearDown(self):
        self.db.close()

    def _open_and_store(self, key: str, value: object):
        """Open a connection, store a value, commit, and close."""
        tm = transaction.TransactionManager()
        conn = self.db.open(tm)
        conn.root()[key] = value
        tm.commit()
        conn.close()
        return conn

    def test_cache_size_reflects_loaded_objects(self):
        # After loading objects through a connection, cache_size is > 0.
        self._open_and_store("x", 42)

        metrics = _metrics(self.collector)
        self.assertGreater(
            _value(metrics, "zodb_cache_size"),
            0.0,
            "expected total cache_size > 0 after storing an object",
        )

    def test_cache_non_ghost_count_after_load(self):
        # After accessing an object's attribute (unghosting it), the
        # non-ghost count is > 0.
        tm = transaction.TransactionManager()
        conn = self.db.open(tm)
        conn.root()["y"] = 99
        tm.commit()
        # Access the value to ensure the object is fully loaded (non-ghost).
        _ = conn.root()["y"]
        conn.close()

        metrics = _metrics(self.collector)
        self.assertGreater(
            _value(metrics, "zodb_cache_non_ghost_count"),
            0.0,
            "expected total cache_non_ghost_count > 0 after unghosting",
        )

    def test_cache_metrics_have_no_labels(self):
        # Cache metrics are summed across all connections — no per-connection
        # labels, which would be unstable across scrapes.
        self._open_and_store("label_test", 1)
        metrics = _metrics(self.collector)
        for sample in metrics["zodb_cache_size"]:
            self.assertEqual(
                sample.labels, {},
                "zodb_cache_size should have no labels",
            )
        for sample in metrics["zodb_cache_non_ghost_count"]:
            self.assertEqual(
                sample.labels, {},
                "zodb_cache_non_ghost_count should have no labels",
            )

    def test_cache_metrics_single_sample(self):
        # Regardless of how many connections are open, there is exactly one
        # sample per cache metric (the aggregate sum).
        tm1 = transaction.TransactionManager()
        conn1 = self.db.open(tm1)
        tm_extra = transaction.TransactionManager()
        conn_extra = self.db.open(tm_extra)

        metrics = _metrics(self.collector)
        self.assertEqual(len(metrics["zodb_cache_size"]), 1)
        self.assertEqual(len(metrics["zodb_cache_non_ghost_count"]), 1)

        tm1.abort()
        conn1.close()
        tm_extra.abort()
        conn_extra.close()

    def test_cache_size_sums_across_connections(self):
        # With two connections open the summed value is >= either alone.
        tm1 = transaction.TransactionManager()
        conn1 = self.db.open(tm1)
        conn1.root()["a"] = 1
        tm1.commit()

        tm2 = transaction.TransactionManager()
        conn2 = self.db.open(tm2)
        conn2.root()["b"] = 2
        tm2.commit()

        single_conn_metrics = _metrics(self.collector)
        summed = _value(single_conn_metrics, "zodb_cache_size")
        self.assertGreaterEqual(summed, 1.0)

        tm1.abort()
        conn1.close()
        tm2.abort()
        conn2.close()


class TestActivityMonitorInstallation(unittest.TestCase):

    def setUp(self):
        self.db = _make_db()

    def tearDown(self):
        self.db.close()

    def test_default_installs_cumulative_monitor(self):
        registry = CollectorRegistry()
        ZODBCollector(self.db, registry=registry)
        self.assertIsInstance(
            self.db.getActivityMonitor(), CumulativeActivityMonitor
        )

    def test_install_false_leaves_monitor_as_none(self):
        registry = CollectorRegistry()
        ZODBCollector(
            self.db,
            registry=registry,
            install_activity_monitor=False)
        self.assertIsNone(self.db.getActivityMonitor())

    def test_existing_monitor_not_overwritten(self):
        existing = CumulativeActivityMonitor()
        self.db.setActivityMonitor(existing)
        registry = CollectorRegistry()
        ZODBCollector(self.db, registry=registry)
        self.assertIs(self.db.getActivityMonitor(), existing)

    def test_registry_none_skips_registration(self):
        collector = ZODBCollector(
            self.db,
            registry=None,
            install_activity_monitor=False)
        # If not registered, the registry has no collectors for our metrics.
        registry = CollectorRegistry()
        names = {m.name for m in registry.collect()}
        self.assertNotIn("zodb_connection_pool_count", names)

    def test_registry_registration(self):
        registry = CollectorRegistry()
        ZODBCollector(
            self.db,
            registry=registry,
            install_activity_monitor=False)
        names = {m.name for m in registry.collect()}
        self.assertIn("zodb_connection_pool_count", names)


class TestActivityMetrics(unittest.TestCase):

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(self.db, registry=self.registry)
        # monitor is now a CumulativeActivityMonitor
        self.monitor: CumulativeActivityMonitor = self.db.getActivityMonitor()

    def tearDown(self):
        self.db.close()

    def _do_transaction(self, key: str, value: object):
        tm = transaction.TransactionManager()
        conn = self.db.open(tm)
        conn.root()[key] = value
        tm.commit()
        conn.close()  # triggers closedConnection on the monitor

    def test_activity_metrics_present(self):
        self._do_transaction("a", 1)
        metrics = _metrics(self.collector)
        # CounterMetricFamily appends _total automatically
        self.assertIn("zodb_loads", metrics)
        self.assertIn("zodb_stores", metrics)
        self.assertIn("zodb_connections", metrics)

    def test_loads_incremented_after_unghosting(self):
        # zodb_loads_total counts objects loaded from storage (unghosted).
        # This only happens on a fresh connection that has not yet cached
        # the object.  Write an object, then read it back through a new
        # connection (no warm cache) to force a storage load.
        self._do_transaction("load_test", {"nested": True})

        # Open a second, independent connection so the object is not in cache.
        db2 = _make_db()
        # Copy the storage so db2 shares the same data as self.db.
        # Easiest: just verify loads on self.db by closing all pooled
        # connections and re-opening to cold-load.
        db2.close()

        # Force a cold load: minimize all caches, then access the object.
        self.db.cacheMinimize()
        monitor_before = self.monitor.loads

        tm = transaction.TransactionManager()
        conn = self.db.open(tm)
        _ = conn.root()["load_test"]   # triggers unghosting → load
        tm.abort()
        conn.close()

        metrics = _metrics(self.collector)
        self.assertGreater(_value(metrics, "zodb_loads"), monitor_before)

    def test_stores_incremented_after_commit(self):
        self._do_transaction("b", 2)
        metrics = _metrics(self.collector)
        self.assertGreater(_value(metrics, "zodb_stores"), 0.0)

    def test_connections_incremented_after_close(self):
        self._do_transaction("c", 3)
        metrics = _metrics(self.collector)
        self.assertGreater(_value(metrics, "zodb_connections"), 0.0)

    def test_counters_accumulate_across_connections(self):
        self._do_transaction("d", 4)
        metrics_1 = _metrics(self.collector)
        stores_1 = _value(metrics_1, "zodb_stores")
        connections_1 = _value(metrics_1, "zodb_connections")

        self._do_transaction("e", 5)
        metrics_2 = _metrics(self.collector)
        stores_2 = _value(metrics_2, "zodb_stores")
        connections_2 = _value(metrics_2, "zodb_connections")

        self.assertGreater(stores_2, stores_1)
        self.assertGreater(connections_2, connections_1)

    def test_activity_metrics_absent_without_cumulative_monitor(self):
        # If a non-CumulativeActivityMonitor is installed (or none), the
        # activity counters are not yielded.
        db2 = _make_db()
        registry2 = CollectorRegistry()
        collector2 = ZODBCollector(
            db2, registry=registry2, install_activity_monitor=False
        )
        metrics = _metrics(collector2)
        self.assertNotIn("zodb_loads", metrics)
        self.assertNotIn("zodb_stores", metrics)
        self.assertNotIn("zodb_connections", metrics)
        db2.close()


class TestMetricNames(unittest.TestCase):
    """Guard against naming regressions."""

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(self.db, registry=self.registry)

    def tearDown(self):
        self.db.close()

    def test_gauge_names(self):
        metrics = _metrics(self.collector)
        for name in (
            "zodb_connection_pool_count",
            "zodb_cache_size",
                "zodb_cache_non_ghost_count"):
            self.assertIn(name, metrics)

    def test_counter_names(self):
        # CounterMetricFamily stores samples under name + "_total"
        metrics = _metrics(self.collector)
        for name in ("zodb_loads", "zodb_stores", "zodb_connections"):
            self.assertIn(name, metrics)
            sample_names = {s.name for s in metrics[name]}
            self.assertIn(name + "_total", sample_names)

    def test_gauge_types(self):
        for m in self.collector.collect():
            if m.name in (
                "zodb_connection_pool_count",
                "zodb_cache_size",
                    "zodb_cache_non_ghost_count"):
                self.assertIsInstance(m, GaugeMetricFamily)

    def test_counter_types(self):
        for m in self.collector.collect():
            if m.name in ("zodb_loads", "zodb_stores", "zodb_connections"):
                self.assertIsInstance(m, CounterMetricFamily)


class TestCumulativeActivityMonitorDirectly(unittest.TestCase):
    """Unit tests for CumulativeActivityMonitor in isolation."""

    def test_initial_state(self):
        monitor = CumulativeActivityMonitor()
        self.assertEqual(monitor.loads, 0)
        self.assertEqual(monitor.stores, 0)
        self.assertEqual(monitor.connections, 0)

    def test_accumulates_across_connections(self):
        db = _make_db()
        monitor = CumulativeActivityMonitor()
        db.setActivityMonitor(monitor)

        def do_work(key, value):
            tm = transaction.TransactionManager()
            conn = db.open(tm)
            conn.root()[key] = value
            tm.commit()
            conn.close()

        do_work("p", 10)
        stores_after_1 = monitor.stores
        connections_after_1 = monitor.connections
        self.assertEqual(connections_after_1, 1)
        self.assertGreater(stores_after_1, 0)

        do_work("q", 20)
        self.assertEqual(monitor.connections, 2)
        self.assertGreater(monitor.stores, stores_after_1)

        db.close()

    def test_transfer_counts_reset_on_connection_reuse(self):
        # After a connection is returned to the pool and reused, its per-
        # connection transfer counts start from zero (clear=True in
        # getTransferCounts).  The cumulative monitor should therefore not
        # double-count.
        db = _make_db()
        monitor = CumulativeActivityMonitor()
        db.setActivityMonitor(monitor)

        tm = transaction.TransactionManager()
        conn = db.open(tm)
        conn.root()["r"] = 1
        tm.commit()
        conn.close()  # returns to pool; closedConnection called once

        stores_after_first = monitor.stores

        # Reopen (likely reuses same connection object from pool)
        tm2 = transaction.TransactionManager()
        conn2 = db.open(tm2)
        conn2.root()["s"] = 2
        tm2.commit()
        conn2.close()  # closedConnection called again

        # Stores should have grown, not doubled the first value
        self.assertEqual(monitor.connections, 2)
        self.assertGreater(monitor.stores, stores_after_first)

        db.close()


class TestConfigMetrics(unittest.TestCase):
    """Pool/cache configuration gauges and storage size."""

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(
            self.db, registry=self.registry, install_activity_monitor=False
        )

    def tearDown(self):
        self.db.close()

    def test_pool_size_limit_matches_configured_value(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_connection_pool_size", metrics)
        self.assertEqual(
            _value(metrics, "zodb_connection_pool_size"),
            float(self.db.getPoolSize()),
        )

    def test_cache_size_limit_matches_configured_value(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_cache_size_limit", metrics)
        self.assertEqual(
            _value(metrics, "zodb_cache_size_limit"),
            float(self.db.getCacheSize()),
        )

    def test_cache_size_bytes_limit_present(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_cache_size_bytes_limit", metrics)

    def test_db_size_bytes_present_and_non_negative(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_db_size_bytes", metrics)
        self.assertGreaterEqual(_value(metrics, "zodb_db_size_bytes"), 0.0)


class TestHistoricalPoolMetrics(unittest.TestCase):
    """Historical (read-only at-a-point-in-time) connection pool metrics."""

    def setUp(self):
        self.db = _make_db()
        self.registry = CollectorRegistry()
        self.collector = ZODBCollector(
            self.db, registry=self.registry, install_activity_monitor=False
        )

    def tearDown(self):
        self.db.close()

    def test_historical_pool_size_present(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_historical_connection_pool_count", metrics)

    def test_historical_pool_size_zero_with_no_historical_connections(self):
        metrics = _metrics(self.collector)
        self.assertEqual(
            _value(metrics, "zodb_historical_connection_pool_count"), 0.0
        )

    def test_historical_pool_size_limit_matches_configured_value(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_historical_connection_pool_size", metrics)
        self.assertEqual(
            _value(metrics, "zodb_historical_connection_pool_size"),
            float(self.db.getHistoricalPoolSize()),
        )

    def test_historical_cache_size_limit_matches_configured_value(self):
        metrics = _metrics(self.collector)
        self.assertIn("zodb_historical_cache_size_limit", metrics)
        self.assertEqual(
            _value(metrics, "zodb_historical_cache_size_limit"),
            float(self.db.getHistoricalCacheSize()),
        )


if __name__ == "__main__":
    unittest.main()
