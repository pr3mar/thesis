import code.config as config
from snowflake.connector import connect, SnowflakeConnection


class SnowflakeWrapper:
    def __init__(self, connection):
        self.__connection = connection
        self.__cursor = self.__connection.cursor()

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    def execute_query(self, query: str):
        return self.__cursor.execute(query).fetchall()

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
