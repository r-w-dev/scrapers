"""
Postgresql class.

@author: Roel de Vries
@email: rwdevries89@gmail.com
"""

from sqlalchemy import (Column, Float, Integer, MetaData, Table, Text,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DIALECT = 'postgresql'
USER = ''
PW = ''
ADDRESS = ''
PORT = '5432'
DB = 'toerisme'


class Psql:
    """Postgresql database class."""

    engine = None
    sessie_maker = None
    dec_base = None
    schema = None

    def __init__(self, schema: str):
        self.schema = schema

    @staticmethod
    def _create_pg_engine(echo: bool):
        db_url = '{0}://{1}:{2}@{3}:{4}/{5}'.format(
            DIALECT, USER, PW, ADDRESS, PORT, DB)
        return create_engine(db_url, echo=echo)

    def set_sess_maker(self):
        """Initialize sessionmaker."""
        self.sessie_maker = sessionmaker()
        return self

    def set_engine(self, echo: bool = False):
        """Initialize engine."""
        self.engine = self._create_pg_engine(echo)
        return self

    def print_engine(self):
        """Print engine connection."""
        print(self.engine)

    def set_dec_base(self, echo=False):
        """Connect declarative base to engine."""
        if self.engine is None:
            self.engine = self._create_pg_engine(echo)

        if self.schema is not None:
            self.engine.execute(
                'CREATE SCHEMA IF NOT EXISTS {0};'.format(self.schema))
            return declarative_base(
                metadata=MetaData(schema=self.schema), bind=self.engine)

        else:
            return declarative_base(bind=self.engine)

    def _create_io_from_data(self, df):
        from io import StringIO

        output = StringIO()
        df.to_csv(output, sep=';', header=False,
                  encoding='utf-8', index=False)
        output.getvalue()
        output.seek(0)
        return output

    def fill_table(self, df, schema, table, echo: bool = True):

        schema_table = '{0}.{1}'.format(schema, table) \
                       if schema is not None else table

        if self.engine is None:
            self.engine = self._create_pg_engine(echo)

        with self.engine.connect() as conn:
            cur = conn.connection.cursor()
            with self._create_io_from_data(df) as f:
                cur.copy_from(f, schema_table, null='', sep=';')
            conn.connection.commit()
            conn.close()

        print('Filled: {0}'.format(schema_table))

    @staticmethod
    def _replace_bad_chars(line: str) -> str:
        ch_o = [' ', '/', '-', '<', '>', '(', ')']
        ch_r = ['_', '_', '_', '', '', '_', '']
        for i in range(7):
            line = line.lower().replace(ch_o[i], ch_r[i])
        return line

    @staticmethod
    def _sql_type(dtype: str):
        if 'int' in str(dtype):
            return Integer
        if 'object' in str(dtype):
            return Text
        if 'float' in str(dtype):
            return Float
        else:
            return Text

    def create_empty_table_from_data(self, df, schema, table,
                                     drop: bool = False):
        columns = [self._replace_bad_chars(c) for c in df.columns]
        sql_dtypes = [self._sql_type(t) for t in df.dtypes]
        cols = [Column(name, sql_dtype)
                for name, sql_dtype in zip(columns, sql_dtypes)]

        with self.engine.connect() as conn:
            table = Table(table, MetaData(conn, schema=schema), *cols)
            if drop:
                table.drop(conn)
                print('table {0} dropped'.format(table))

            table.create(checkfirst=True)
        print('Created: {0}'.format(table))

    def exec_sql(self, sql: str):
        """Execute sql statement on current engine."""
        from sqlalchemy import text
        self.engine.execute(text(sql).execution_options(autocommit=True))
