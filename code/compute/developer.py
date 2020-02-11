import code.config
import datetime
from code.db.utils import SnowflakeWrapper


def get_cards(sw: SnowflakeWrapper, interval: (datetime, datetime)):
    # Use this query:
    # SELECT
    #     KEY,
    #     ARRAY_AGG(
    #         OBJECT_CONSTRUCT(
    #             'author', USERID,
    #             'dateCreated', DATECREATED,
    #             'changelogItem', CHANGELOGITEM
    #             )
    #         ) CHANGELOGITEMS
    # FROM CHANGELOGS
    # WHERE
    #     CHANGELOGITEM:field ILIKE 'status'
    #     AND DATECREATED >= '2019-10-01'
    #     AND DATECREATED < '2020-01-01'
    # GROUP BY KEY;
    return 0


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        result = sw.execute_query("SELECT * FROM USERS LIMIT 10")
        print(result)
        result = sw.execute_query("SELECT * FROM CHANGELOGS LIMIT 10")
        print(result)
