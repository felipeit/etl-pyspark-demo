from sqlalchemy import create_engine


class DbSQLAlchemy:
    def __init__(self) -> None:
        self.__engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
        
    def save(self) -> None:
        pass