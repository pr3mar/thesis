import code.config
import datetime
from code.db.utils import SnowflakeWrapper


def get_authored_activity(sw: SnowflakeWrapper, interval: (datetime, datetime), user_id=None):
    """
    includes comments, changelogs where

    :param sw: SnowflakeWrapper
    :param interval: [start, end)
    :param user_id: ID of a user [if not given gets all users]
    :return:
    """
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
