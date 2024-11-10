from src.domain.ports.loaders import Loader


class DatabaseLoader(Loader):
    def __init__(self, repo: Loader = Loader()) -> None:
        self._repo = repo
        
    def run(self, df) -> None:
       self._repo.save()
    