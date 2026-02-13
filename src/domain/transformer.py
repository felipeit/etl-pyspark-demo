"""Compatibility exports for `src.domain.transformer` used by tests.

Re-exports Transformer port and JSONTransformer adapter.
"""
from src.domain.adapters.transformer import JSONTransformer
from src.domain.ports.transformers import Transformer

__all__ = ["JSONTransformer", "Transformer"]
