import code.config
from code.db.utils import SnowflakeWrapper


def get_cards(sw: SnowflakeWrapper, interval):
    return 0


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        result = sw.execute_query("SELECT * FROM USERS LIMIT 10")
        print(result)
        result = sw.execute_query("SELECT * FROM CHANGELOGS LIMIT 10")
        print(result)
