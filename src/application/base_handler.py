from dataclasses import dataclass
from typing import Any


@dataclass
class Input:
    extractor: Any
    transformer: Any
    loader: Any
