"""Compatibility exports for `src.domain.loader` used by tests.

Re-exports Loader port and DatabaseLoader adapter.
"""
from src.domain.adapters.loader import DatabaseLoader
from src.domain.ports.loaders import Loader

__all__ = ["DatabaseLoader", "Loader"]
