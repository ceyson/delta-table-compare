"""
Engine registry for the recon framework.

Supported engines:
    - "spark": PySpark + Delta Lake (default, production on Databricks)
    - "polars": Polars + delta-rs (local/CI alternative)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import ReconEngine

_ENGINE_REGISTRY: dict[str, type] = {}


def register_engine(name: str, engine_cls: type) -> None:
    """Register an engine class by name."""
    _ENGINE_REGISTRY[name] = engine_cls


def get_engine(name: str, **kwargs) -> "ReconEngine":
    """Instantiate and return an engine by name."""
    if name not in _ENGINE_REGISTRY:
        # Try lazy import
        if name == "spark":
            from .spark_engine import SparkEngine
            register_engine("spark", SparkEngine)
        elif name == "polars":
            from .polars_engine import PolarsEngine
            register_engine("polars", PolarsEngine)
        else:
            raise ValueError(f"Unknown engine: {name!r}. Available: spark, polars")

    return _ENGINE_REGISTRY[name](**kwargs)


__all__ = ["get_engine", "register_engine"]
