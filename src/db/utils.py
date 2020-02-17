import pandas as pd
import src.config as config
from snowflake.connector import connect, SnowflakeConnection
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


class SnowflakeWrapper:
    def __init__(self, connection):
        self.__connection = connection
        self.__cursor = self.__connection.cursor()

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.__cursor.execute(query).fetch_pandas_all()

    def execute(self, query: str) -> None:
        self.__cursor.execute(query)

    @staticmethod
    def execute_df_query(df, table, ifexists='fail'):
        en = SnowflakeWrapper.create_snowflake_engine()
        with en.connect() as conn:
            df.to_sql(table, con=en, index=False, if_exists=ifexists)
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
