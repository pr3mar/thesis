import pandas as pd
import src.config as config
from typing import Union
from snowflake.connector import connect, SnowflakeConnection, DictCursor
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


class SnowflakeWrapper:
    def __init__(self, connection):
        self.__connection = connection
        self.__cursor: DictCursor = self.__connection.cursor()

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    def fetch_df(self, query: str) -> Union[list, pd.DataFrame]:
        return self.__cursor.execute(query).fetch_pandas_all()

    def fetch(self, query, single_row=False):
        if single_row:
            return self.__cursor.execute(query).fetchone()
        else:
            return self.__cursor.execute(query).fetchall()

    def execute(self, query: str) -> None:
        self.__cursor.execute(query)

    @staticmethod
    def execute_df_query(df, table, ifexists='fail'):
        en = SnowflakeWrapper.create_snowflake_engine()
        with en.connect() as conn:
            df.to_sql(table, con=en, index=False, if_exists=ifexists, chunksize=10000)
        en.dispose()

    @staticmethod
    def create_snowflake_connection() -> SnowflakeConnection:
        return connect(
            account=config.snowflakeAccount,
            user=config.snowflakeUsername,
            password=config.snowflakePassword,
            role=config.snowflakeRole,
            warehouse=config.snowflakeWarehouse,
            database=config.snowflakeDatabase,
            schema=config.snowflakeSchema,
        )

    @staticmethod
    def create_snowflake_engine():  # -> Connection:
        return create_engine(URL(
            account=config.snowflakeAccount,
            user=config.snowflakeUsername,
            password=config.snowflakePassword,
            role=config.snowflakeRole,
            warehouse=config.snowflakeWarehouse,
            database=config.snowflakeDatabase,
            schema=config.snowflakeSchema,
        ))
