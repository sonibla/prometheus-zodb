"""Tests for ZODB connection pool available metric."""
from ZODB import DB
from ZODB.MappingStorage import MappingStorage as MemoryStorage
from prometheus_zodb import ZODBCollector


def test_zodb_connection_pool_available_metric():
    """Test that ZODB collector includes available connections metric."""
    db = DB(MemoryStorage(), pool_size=10)
    collector = ZODBCollector(db, registry=None)

    # Open a connection to populate the pool
    conn = db.open()

    # Collect metrics
    metrics = list(collector.collect())

    # Find the available connections metric
    metric_names = [m.name for m in metrics]
    assert "zodb_connection_pool_available" in metric_names

    # Get the metrics
    available_metric = next(m for m in metrics if m.name ==
                            "zodb_connection_pool_available")
    pool_count_metric = next(
        m for m in metrics if m.name == "zodb_connection_pool_count")
    pool_size_metric = next(
        m for m in metrics if m.name == "zodb_connection_pool_size")

    # Should have one sample each
    assert len(available_metric.samples) == 1
    assert len(pool_count_metric.samples) == 1
    assert len(pool_size_metric.samples) == 1

    # Pool size is 10, 1 connection open, so available should be 9
    assert pool_size_metric.samples[0].value == 10.0
    assert pool_count_metric.samples[0].value == 1.0
    assert available_metric.samples[0].value == 9.0

    # Clean up
    conn.close()
    db.close()


def test_zodb_connection_pool_available_with_multiple_connections():
    """Test available metric with multiple open connections."""
    db = DB(MemoryStorage(), pool_size=5)
    collector = ZODBCollector(db, registry=None)

    # Open 3 connections
    conn1 = db.open()
    conn2 = db.open()
    conn3 = db.open()

    # Collect metrics
    metrics = list(collector.collect())

    # Get metrics
    pool_count_metric = next(
        m for m in metrics if m.name == "zodb_connection_pool_count")
    pool_size_metric = next(
        m for m in metrics if m.name == "zodb_connection_pool_size")
    available_metric = next(m for m in metrics if m.name ==
                            "zodb_connection_pool_available")

    # Should have 3 connections open, max 5
    assert pool_count_metric.samples[0].value == 3.0
    assert pool_size_metric.samples[0].value == 5.0

    # Available should be 5 - 3 = 2
    assert available_metric.samples[0].value == 2.0

    # Clean up
    conn1.close()
    conn2.close()
    conn3.close()
    db.close()
