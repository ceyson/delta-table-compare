import pytest


def test_config_import():
    from recon.config import ReconcileConfig

    assert ReconcileConfig is not None


@pytest.mark.spark
def test_recon_imports():
    from recon.config import ReconcileConfig
    from recon import helpers
    from recon import runner

    assert ReconcileConfig is not None
    assert helpers is not None
    assert runner is not None


@pytest.mark.spark
def test_runner_exports_run_reconciliation():
    from recon.runner import run_reconciliation

    assert callable(run_reconciliation)


@pytest.mark.polars
def test_polars_engine_import():
    from recon.engines import get_engine

    engine = get_engine("polars")
    assert engine is not None
