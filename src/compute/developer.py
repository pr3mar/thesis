import json
import pickle
import os
import matplotlib.pyplot as plt
import pandas as pd
import src.config as config
from pandas import DataFrame
import src.compute.utils as utils
from collections import defaultdict
from typing import Union
from datetime import date
from src.db.utils import SnowflakeWrapper
from src.compute.utils import mask_in, Interval


def get_developer_ids(sw: SnowflakeWrapper) -> list:
    return sw.execute_query(
        "SELECT USERKEY KEY "
        "FROM USERS "
        "WHERE ACTIVE = TRUE AND USERKEY NOT ILIKE '%addon%'"
        "ORDER BY 1;"
    )['KEY'].tolist()


def get_distinct_statuses(sw: SnowflakeWrapper) -> list:
    return sw.execute_query(
        "SELECT ID "
        "FROM STATUSES "
        "ORDER BY 1;"
    )['ID'].tolist()


def get_aggregated_authored_activity(sw: SnowflakeWrapper, interval: Interval, user_id: Union[None, list] = None):
    ids = "" if user_id is None else f" USERID IN ({mask_in(user_id)}) AND"
    result = sw.execute_query(
        f"SELECT "
        f"    USERID, "
        f"    ARRAY_AGG( "
        f"        OBJECT_CONSTRUCT( "
        f"            'userId', USERID, "
        f"            'field', FIELD, "
        f"            'count', CNT "
        f"            ) "
        f"        ) ACTIVITY "
        f"FROM ( "
        f"    SELECT "
        f"        activity.USERID, "
        f"        item.VALUE:field::string FIELD, "
        f"        COUNT(*) CNT "
        f"    FROM "
        f"         (SELECT "
        f"            USERID, "
        f"            ARRAY_AGG(CHANGELOGITEM) CHANGELOGITEMS "
        f"        FROM CHANGELOGS "
        f"        WHERE "
        f"            {ids} "
        f"            DATECREATED >= {interval.fromDate()} "
        f"            AND DATECREATED < {interval.toDate()} "
        f"        GROUP BY USERID) activity, "
        f"         lateral flatten(activity.CHANGELOGITEMS) item "
        f"    GROUP BY 1, 2 "
        f") "
        f"GROUP BY 1 "
        f"ORDER BY 1; "
    )
    return pd.concat(
        result['ACTIVITY'].apply(
            lambda x:
            pd.DataFrame(json.loads(x))
                .pivot(index='userId', columns='field', values='count')).tolist(),  # TODO Add breakdown by issue type
        sort=True)


def get_authored_activity(sw: SnowflakeWrapper, interval: Interval, user_id: Union[None, list] = None):
    """
    includes comments, changelogs where the [optional] userId has performed the changes

    :param sw: SnowflakeWrapper
    :param interval: [start, end)
    :param user_id: ID of a user [if not given gets all users]
    :return:
    """
    ids = "" if user_id is None else f" USERID IN ({mask_in(user_id)}) AND "
    return sw.execute_query(
        f"SELECT "
        f"    USERID, "
        f"    ARRAY_AGG( "
        f"        OBJECT_CONSTRUCT( "
        f"            'key', KEY, "
        f"            'dateCreated', DATECREATED, "
        f"            'changelogItem', CHANGELOGITEM "
        f"            ) "
        f"        ) CHANGELOGITEMS "
        f"FROM CHANGELOGS "
        f"WHERE "
        f"    {ids} "
        f"    DATECREATED >= {interval.fromDate()} AND "
        f"    DATECREATED < {interval.toDate()} "
        f"GROUP BY USERID;"
    )


def get_developer(sw: SnowflakeWrapper, interval: Interval, userId: str) -> DataFrame:
    return sw.execute_query(
        f" SELECT "
        f"     id AS STATUS, "
        f"     UNIQUEKEYS, "
        f"     COUNT, "
        f"     AVG_DAY, "
        f"     MAX_DAYS, "
        f"     MIN_DAYS "
        f" FROM STATUSES s "
        f" LEFT JOIN ( "
        f"     SELECT "
        f"         STATUS, "
        f"         COUNT(DISTINCT KEY)             UNIQUEKEYS, "
        f"         COUNT(*)                        COUNT, "
        f"         AVG(TIMEDELTA) / (60 * 60 * 24) AVG_DAY, "
        f"         MAX(TIMEDELTA) / (60 * 60 * 24) MAX_DAYS, "
        f"         MIN(TIMEDELTA) / (60 * 60 * 24) MIN_DAYS "
        f"     FROM TIMELINES "
        f"     WHERE "
        f"         DATEFROM >= {interval.fromDate()} "
        f"         AND DATETO < {interval.toDate()} "
        f"         AND ASSIGNEE = '{userId}' "
        f"     GROUP BY "
        f"         STATUS "
        f"     ORDER BY 1 "
        f" ) t ON t.STATUS = s.id; "
    )


def get_average_developer(sw: SnowflakeWrapper, interval: Interval, use_cached: bool = True) -> list:
    fname = f"{config.data_root}/avg_devs.pkl"
    if use_cached and os.path.isfile(fname):
        statuses = pickle.load(fname, encoding='utf8')
    else:
        statuses = {s: DataFrame(columns=["STATUS", "UNIQUEKEYS", "COUNT", "AVG_DAY", "MAX_DAYS", "MIN_DAYS"]) for s in
                    get_distinct_statuses(sw)}
        for user_id in get_developer_ids(sw):
            print(user_id)
            for _, row in get_developer(sw, interval, user_id).iterrows():
                statuses[row['STATUS']] = statuses[row['STATUS']].append(row, ignore_index=True)
        with open(fname, "wb") as out_file:
            pickle.dump(statuses, out_file)
    return statuses


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        # result = get_authored_activity(sw, (date(2019, 10, 1), date(2020, 1, 1)), ['andrej.oblak'])
        # result = get_aggregated_authored_activity(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))  # , ['andrej.oblak'])
        # plt.figure()
        # result.hist('status', bins=40)
        # plt.show()
        # dev = get_developer(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)), 'marko.prelevikj')
        avg_dev = get_average_developer(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))
        print(avg_dev)
