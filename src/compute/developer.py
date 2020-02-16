import json
import matplotlib.pyplot as plt
import pandas as pd
import src.compute.utils as utils
from collections import defaultdict
from typing import Union
from datetime import date, datetime
from src.db.utils import SnowflakeWrapper
from src.compute.utils import mask_in, Interval


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


def get_work_activity(sw: SnowflakeWrapper, interval: Interval, user_id: Union[None, list] = None) -> (dict, dict):
    def extract_users_involved(changelogs: pd.DataFrame) -> dict:
        users = defaultdict(set)
        for _, row in changelogs.iterrows():
            key = row["KEY"]
            for logs in row["CHANGELOGITEMS"]:
                for log in logs["changelogItems"]:
                    if log["field"] != "assignee":
                        continue
                    if "from" in log:
                        users[log["from"]].add(key)
                    if "to" in log:
                        users[log["to"]].add(key)
        return dict(users)

    def user_breakdown(changelogWTimelines: pd.DataFrame) -> pd.DataFrame:
        changelogWTimelines['timelines'] = changelogWTimelines['timelines'].apply(
            lambda x:
            pd.DataFrame(x)
                .groupby('assignee', as_index=False)['status', 'date_from', 'date_to']
                .agg((lambda y: list(y)))
        )
        return changelogWTimelines
        # return changelogWTimelines #, changelogWTimelines.groupby('assignee', as_index=False)['status', 'date_from', 'date_to'].agg({'list': (lambda x: list(x))})

    changelogs = utils.work_activity_on_interval(sw, interval)
    users = extract_users_involved(changelogs)
    users_breakdown = user_breakdown(utils.build_issue_timelines(sw, interval))
    return users, users_breakdown


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        # result = get_authored_activity(sw, (date(2019, 10, 1), date(2020, 1, 1)), ['andrej.oblak'])
        # result = get_aggregated_authored_activity(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))  # , ['andrej.oblak'])
        # plt.figure()
        # result.hist('status', bins=40)
        # plt.show()
        result, timelines = get_work_activity(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))  # , ['andrej.oblak'])
        print(result)
