from src.domain.ports.loaders import Loader


class CrytoRepository(Loader):
    def save(self) -> None:
        ...