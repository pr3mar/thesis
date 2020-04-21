import json
import os
import pickle
from datetime import date
from typing import Union

import pandas as pd
import numpy as np
from pandas import DataFrame

import src.config as config
from src.compute.utils import mask_in, Interval
from src.config import data_root
from src.db.utils import SnowflakeWrapper


def get_developer_ids(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df(
        "SELECT USERKEY KEY "
        "FROM USERS "
        "WHERE ACTIVE = TRUE AND USERKEY NOT ILIKE '%addon%'"
        "ORDER BY 1;"
    )['KEY'].tolist()


def get_distinct_statuses(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df(
        "SELECT ID "
        "FROM STATUSES "
        "ORDER BY 1;"
    )['ID'].tolist()


def get_avg_authored_activity(sw: SnowflakeWrapper, interval: Interval) -> pd.DataFrame:
    activity = get_aggregated_authored_activity(sw, interval)
    return activity.fillna(0).describe().T.sort_values(by="mean", ascending=False)


def get_aggregated_authored_activity(sw: SnowflakeWrapper, interval: Interval, user_id: Union[None, list] = None):
    ids = "" if user_id is None else f" USERID IN ({mask_in(user_id)}) AND"
    result = sw.fetch_df(
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
    return sw.fetch_df(
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
    df = sw.fetch_df(
        f" SELECT "
        f"     id AS STATUS, "
        f'     "UniqueIssues", '
        f'     "Issues", '
        f'     "Reassignments", '
        f"     AVG_DAY, "
        f"     MAX_DAYS, "
        f"     MIN_DAYS "
        f" FROM STATUSES s "
        f" LEFT JOIN ( "
        f"     SELECT "
        f"         STATUS, "
        f'         COUNT(DISTINCT KEY)             "UniqueIssues", '
        f'         COUNT(*)                        "Issues", '
        f'         COUNT(*)                        "Reassignments", '
        f"         AVG(TIMEDELTA) / (60 * 60 * 24) AVG_DAY, "
        f"         MAX(TIMEDELTA) / (60 * 60 * 24) MAX_DAYS, "
        f"         MIN(TIMEDELTA) / (60 * 60 * 24) MIN_DAYS "
        f"     FROM TIMELINES "
        f"     WHERE "
        f"         DATEFROM >= {interval.fromDate()} "
        f"         AND (DATETO < {interval.toDate()} OR DATETO IS NULL) "
        f"         AND ASSIGNEE = '{userId}' "
        f"     GROUP BY "
        f"         STATUS "
        f"     ORDER BY 1 "
        f" ) t ON t.STATUS = s.id; "
    )
    df["AVG_DAY"] = df["AVG_DAY"].map(lambda x: np.nan if x is None else float(x))
    return df


def get_all_developers_by_status(sw: SnowflakeWrapper, interval: Interval, use_cached: bool = True) -> dict:
    fname = f"{config.data_root}/by_status/avg_devs-{interval.fname()}.pkl"
    if use_cached and os.path.isfile(fname):
        with open(fname, 'rb') as file:
            return pickle.load(file, encoding='utf8')
    else:
        statuses = {s: DataFrame(columns=["STATUS", "USERID", "UniqueIssues", "Issues", "Reassignments", "AVG_DAY", "MAX_DAYS", "MIN_DAYS"])
                    for s in
                    get_distinct_statuses(sw)}
        for user_id in get_developer_ids(sw):
            print(f"Working...")
            for _, row in get_developer(sw, interval, user_id).iterrows():
                row = pd.concat([pd.Series([user_id], index=['USERID']), row])
                statuses[row['STATUS']] = statuses[row['STATUS']].append(row, ignore_index=True)
        with open(fname, "wb") as out_file:
            pickle.dump(statuses, out_file)
    return statuses


def merge_statuses(by_status: dict, merged_statuses: dict) -> dict:
    merged = {}
    for key, statuses in merged_statuses.items():
        merged[key] = pd.concat([by_status[s] for s in statuses], ignore_index=True)
    return merged


def avg_by_status(data: dict, include_nans=False) -> dict:
    avgs = {}
    for k, v in data.items():
        nans = v.isna().sum()[2:]
        if (nans < nans.max()).all():  # status, userId are first 2
            print((nans[1:] < nans[1:].max()).all())
            raise Exception(f"not all vals are max nan {nans}")
        # Snowflake's AVG returns a None if there is a NaN
        v["AVG_DAY"] = v["AVG_DAY"].map(lambda x: pd.np.nan if x is None else float(x))
        avgs[k] = v.fillna(0.).mean() if include_nans else v.mean()
        avgs[k]["NANs"] = nans[1:].max()
        avgs[k]["VALs"] = len(v) - nans[1:].max()
        avgs[k]["ALL"] = len(v)
        avgs[k]["STATUS"] = k
    return avgs


def get_avg_developer(sw: SnowflakeWrapper, interval: Interval, include_nans: bool = False, merge=False) -> (DataFrame):
    fname = f"{data_root}/developer/avg_dev_" \
            f"{interval.fname()}" \
            f"{'_nan' if include_nans else ''}" \
            f"{'_merged' if merge else ''}.pkl"
    with open(fname, "wb") as output_file:
        data_by_status = get_all_developers_by_status(sw, interval)
        if merge:
            file_statuses = open(f"{data_root}/statuses/merged.json")
            merged_statuses = json.load(file_statuses)
            data_by_status = merge_statuses(data_by_status, merged_statuses)
        avg_dev = avg_by_status(data_by_status, include_nans)
        avg_dev_df = DataFrame(avg_dev).T.fillna(0).sort_values("VALs", ascending=False)
        pickle.dump(avg_dev_df, output_file)
        return avg_dev_df


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        interval = Interval(date(2019, 10, 1), date(2020, 1, 1))
        # result = get_authored_activity(sw, interval, ['marko.prelevikj'])
        # plt.figure()
        # result.hist('status', bins=40)
        # plt.show()
        # dev = get_developer(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)), 'marko.prelevikj')
        avg_dev = get_avg_developer(sw, interval, include_nans=False)
        # avg_dev_nan = get_avg_developer(sw, include_nans=True)
