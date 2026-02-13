"""Compatibility exports for older import paths.

Provides: CSVExtractor, Extractor (alias for port Extract)
"""
from src.domain.adapters.extractor import CSVExtractor
from src.domain.ports.extractors import Extract as Extractor

__all__ = ["CSVExtractor", "Extractor"]
