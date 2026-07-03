"""
Multi-engine Data Reconciliation Package.

Public API:
    ReconcileConfig    - Configuration dataclass for a reconciliation run.
    run_reconciliation - Execute a full reconciliation and return output table paths.
    get_spark          - Get or create the active SparkSession.
    set_spark          - Override the SparkSession (for testing).
"""

from recon.config import ReconcileConfig


def run_reconciliation(*args, **kwargs):
    """Lazy wrapper — imports the full runner only when called (requires PySpark)."""
    from recon.runner import run_reconciliation as _run
    return _run(*args, **kwargs)


def get_spark():
    """Lazy wrapper — returns the active SparkSession."""
    from recon.helpers import get_spark as _get
    return _get()


def set_spark(session):
    """Override the module-level SparkSession (used by tests)."""
    from recon.helpers import set_spark as _set
    _set(session)


def get_write_timings():
    """Access the global write timing collector."""
    from recon.helpers import get_write_timings as _get
    return _get()


__all__ = ["ReconcileConfig", "run_reconciliation", "get_spark", "set_spark", "get_write_timings"]
