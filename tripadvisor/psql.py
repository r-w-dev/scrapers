from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DIALECT = "postgresql"
USER = "postgres"
PW = "pq10"
ADDRESS = "192.168.178.3"
PORT = "5432"
DB = "TripAdvisor_scrape"


class Psql:

    engine = None
    sessie_maker = None
    dec_base = None

    @staticmethod
    def _create_pg_engine(echo: bool):
        db_url = "{0}://{1}:{2}@{3}:{4}/{5}".format(
            DIALECT, USER, PW, ADDRESS, PORT, DB)
        return create_engine(db_url, echo=echo)

    def set_sess_maker(self):
        self.sessie_maker = sessionmaker()
        return self

    def print_engine(self):
        print(self.engine)

    def set_dec_base(self, schema=None, echo=False):
        if self.engine is None:
            self.engine = self._create_pg_engine(echo)

        if schema is not None:
            self.engine.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(schema))
            return declarative_base(metadata=MetaData(schema=schema), bind=self.engine)

        else:
            return declarative_base(bind=self.engine)
