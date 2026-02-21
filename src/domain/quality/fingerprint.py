from hashlib import sha256
from pathlib import Path


def build_source_fingerprint(file_path: str, logical_timestamp: str) -> str:
    source = Path(file_path)
    content_hash = sha256(source.read_bytes()).hexdigest()
    payload = f"{content_hash}:{source.stat().st_size}:{logical_timestamp}"
    return sha256(payload.encode("utf-8")).hexdigest()
