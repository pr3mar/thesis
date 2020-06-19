import json
import os
import pickle
from datetime import date, timedelta
from typing import Union

import pandas as pd
import numpy as np
from pandas import DataFrame

import src.config as config
from src.compute.utils import mask_in, Interval, get_distinct_statuses, map_statuses, expand_statuses
from src.config import data_root
from src.db.utils import SnowflakeWrapper


def get_developer_ids(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df(
        "SELECT USERKEY KEY "
        "FROM USERS "
        "WHERE ACTIVE = TRUE AND USERKEY NOT ILIKE '%addon%' AND USERKEY NOT ILIKE 'ID%' "
        "ORDER BY 1;"
    )['KEY'].tolist()


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


def get_developers(
        sw: SnowflakeWrapper,
        interval: Interval,
        user_filters: Union[dict, None] = None,
        break_by: Union[list, None] = None,
        max_duration: int = 30,
        debug: bool = False,
) -> DataFrame:
    if user_filters is None:
        user_filters = {"status": "dev"}
    if break_by is None:
        break_by = ["assignee"]
    else:
        break_by = list(set(["assignee"] + break_by))

    dimensions = {
        "type": ' i.FIELDS:issuetype:name::string "IssueType" ',
        "priority": ' i.FIELDS:priority:name::string  "IssuePriority" ',
        "status": " STATUS ",
        "assignee": " ASSIGNEE ",
    }
    statuses = ','.join([f"'{s}'" for s in expand_statuses(user_filters["status"])]) if "status" in user_filters and \
                                                                                        user_filters["status"] else None
    filters = {
        "status": f"STATUS IN ({statuses})" if statuses else " 1 = 1 ",
        "priority": f'i.FIELDS:priority:name::string IN (\'{user_filters["priority"]}\')' if "priority" in user_filters and
                                                                                             user_filters[
                                                                                                 "priority"] else " 1 = 1 ",
        "type": f'i.FIELDS:issuetype:name::string IN (\'{user_filters["type"]}\')' if "type" in user_filters and
                                                                                      user_filters["type"] else " 1 = 1"
    }
    sql_filters = ' AND '.join(filters.values())
    dims = ','.join([dimensions[dim] for dim in break_by])
    group_by = ','.join([str(i + 1) for i, _ in enumerate(break_by)])
    max_duration = f" AVG_DAYS <= {min(30, max_duration)} " if max_duration > 1 else " AVG_HOURS < 10 "

    sql = (
        f' SELECT * '
        f' FROM ( '
        f'          SELECT '
        f'              {dims}, '
        f'              COUNT(DISTINCT t.KEY)            "UniqueIssues", '
        f'              COUNT(t.KEY)                     "Issues", '
        f'              ("Issues" / "UniqueIssues") - 1  "DegreeOfCycling", '
        f'              AVG(TIMEDELTA) / (60 * 60 * 24)  AVG_DAYS, '
        f'              MAX(TIMEDELTA) / (60 * 60 * 24)  MAX_DAYS, '
        f'              MIN(TIMEDELTA) / (60 * 60 * 24)  MIN_DAYS, '
        f'              AVG(TIMEDELTA) / (60 * 60)       AVG_HOURS, '
        f'              MAX(TIMEDELTA) / (60 * 60)       MAX_HOURS, '
        f'              MIN(TIMEDELTA) / (60 * 60)       MIN_HOURS '
        f'          FROM TIMELINES t INNER JOIN ISSUES i ON t.KEY = i.KEY '
        f'          WHERE DATEFROM >= {interval.fromDate()} '
        f'            AND (DATETO < {interval.toDate()} OR DATETO IS NULL) '
        f'            AND ASSIGNEE IS NOT NULL '
        f'            AND {sql_filters}'
        f'          GROUP BY {group_by} '
        f'      ) '
        f' WHERE AVG_HOURS > 2 '
        f'   AND {max_duration} '
        f' ORDER BY 1 '
    )
    if debug:
        print(sql)
    df = sw.fetch_df(sql)
    df["AVG_DAYS"] = df["AVG_DAYS"].map(lambda x: np.nan if x is None else float(x))
    df["AVG_HOURS"] = df["AVG_HOURS"].map(lambda x: np.nan if x is None else float(x))
    if "status" in break_by:
        df.insert(2, "MERGED_STATUS", map_statuses(df["STATUS"]))
    return df


def get_all_developers_by_status(sw: SnowflakeWrapper, interval: Interval, use_cached: bool = True) -> dict:
    raise Exception("[DEPRECATED] DO NOT USE THIS FUNCTION.")
    fname = f"{config.data_root}/developers/by_status/avg_devs-{interval.fname()}.pkl"
    if use_cached and os.path.isfile(fname):
        with open(fname, 'rb') as file:
            return pickle.load(file, encoding='utf8')
    else:
        statuses = {s: DataFrame(
            columns=["STATUS", "USERID", "UniqueIssues", "Issues", "Reassignments", "AVG_DAY", "MAX_DAYS", "MIN_DAYS"])
                    for s in
                    get_distinct_statuses(sw)}
        for user_id in get_developer_ids(sw):
            print(f"Working...")
            # FIXME this can be done in a single query just like the tickets
            for _, row in get_developers(sw, interval, user_id).iterrows():
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
    fname = f"{data_root}/developers/developer/avg_dev_" \
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


def tickets_assigned_in_interval(sw: SnowflakeWrapper, developer_id: str, interval: Interval) -> pd.DataFrame:
    sql = "SELECT KEY, SUM(1) DAYS_ASSIGNED FROM ("
    days = []
    for i in range((interval.toDate(raw=True) - interval.fromDate(raw=True)).days + 1):
        day = interval.fromDate(raw=True) + timedelta(days=i)
        sql_date = Interval.strdate(day)
        print(sql_date)
        days.append(
            f" SELECT "
            f"   DISTINCT KEY "
            f" FROM TIMELINES "
            f" WHERE "
            f"   ASSIGNEE IN ('{developer_id}') "
            f"   AND ( "
            f"     {sql_date} BETWEEN DATEFROM AND DATETO "
            f"     OR ( "
            f"       DATEDIFF('day', {sql_date}, DATEFROM) = 0 "
            f"       AND DATEDIFF('day', {sql_date}, DATETO) > 1 "
            f"     ) "
            f"   ) "
        )
    sql += " UNION ALL ".join(days) + ") GROUP BY 1 ORDER BY 2, 1;"
    print(sql)
    return sw.fetch_df(sql)


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        interval = Interval(date(2019, 10, 1), date(2020, 1, 1))
        # result = get_authored_activity(sw, interval, ['marko.prelevikj'])
        # plt.figure()
        # result.hist('status', bins=40)
        # plt.show()
        # dev = get_developer(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)), 'marko.prelevikj')
        # avg_dev = get_avg_developer(sw, interval, include_nans=False)
        # avg_dev_nan = get_avg_developer(sw, include_nans=True)
        # assigned_interval = Interval(date(2019, 10, 1), date(2019, 11, 6))
        # assigned_interval = Interval(date(2019, 10, 1), date(2019, 10, 10))
        # assigned = tickets_assigned_in_interval(sw, 'marko.prelevikj', assigned_interval)
        data = get_developers(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)), break_by=[], user_filters={})
