import os
import pickle
from datetime import date
from typing import Union

import numpy as np
from pandas import DataFrame

from src.compute.utils import convert_date, Interval, get_distinct_statuses, map_statuses
from src.config import data_root
from src.db.utils import SnowflakeWrapper


def get_ticket_priorities(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df('SELECT DISTINCT i.FIELDS:priority: name::string "IssuePriority" FROM ISSUES i;')["IssuePriority"].to_list()


def get_ticket_types(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df('SELECT DISTINCT i.FIELDS:issuetype: name::string "IssueType" FROM ISSUES i;')["IssueType"].to_list()


def get_ticket_counts(sw: SnowflakeWrapper, breakdowns: Union[list, None] = None) -> DataFrame:
    if breakdowns is None:
        breakdowns = ["issueType", "issuePriority", "resolved"]
    breakedown = {
        "issueType": "i.fields:issuetype:name::string issueType",
        "issuePriority": "i.fields:priority:name::string issuePriority",
        "resolved": "IFF(i.fields:resolutiondate IS NULL, 'No', 'Yes') Resolved"
    }
    dimensions = [breakedown[by] for by in breakdowns]
    group_by = ','.join([str(i + 1) for i, _ in enumerate(dimensions)])

    sql = (
        f"SELECT "
        f"    {','.join(dimensions)}, "
        f"    count(*) Count "
        f"FROM "
        f"    ISSUES i "
        f"GROUP BY {group_by} "
        f"ORDER BY {len(dimensions) + 1}, {group_by} DESC; ")
    print(sql)
    return sw.fetch_df(sql)


def get_tickets(sw: SnowflakeWrapper, interval: Interval, resolved: bool = True, max_days: int = 10) -> DataFrame:
    resolution_date = f"{convert_date('i.FIELDS:resolutiondate')}"
    where_resolution = f"{resolution_date} >= {interval.fromDate()} AND {resolution_date} < {interval.toDate()}" if resolved else ""
    max_duration = f" AVG_DAYS <= {min(30, max_days)} " if max_days > 1 else " AVG_HOUR < 10 "
    sql = (
        f'SELECT TICKET_KEY, '
        f'       STATUS, '
        f'       "IssueType", '
        f'       "IssuePriority", '
        f'       "States", '
        f'       "Transitions", '
        f'       "DegreeOfCycling", '
        f'       AVG_DAYS, '
        f'       MAX_DAYS, '
        f'       MIN_DAYS, '
        f'       AVG_HOUR, '
        f'       MAX_HOURS, '
        f'       MIN_HOURS '
        f'FROM (SELECT t.KEY                            TICKET_KEY, '
        f'             STATUS, '
        f'             i.FIELDS:issuetype: name::string "IssueType", '
        f'             i.FIELDS:priority: name::string  "IssuePriority", '
        f'             COUNT(DISTINCT t.STATUS)         "States", '
        f'             COUNT(*)                         "Transitions", '
        f'             ("Transitions" / "States") - 1   "DegreeOfCycling", '
        f'             AVG(TIMEDELTA) / (60 * 60 * 24)  AVG_DAYS, '
        f'             MAX(TIMEDELTA) / (60 * 60 * 24)  MAX_DAYS, '
        f'             MIN(TIMEDELTA) / (60 * 60 * 24)  MIN_DAYS, '
        f'             AVG(TIMEDELTA) / (60 * 60)       AVG_HOUR, '
        f'             MAX(TIMEDELTA) / (60 * 60)       MAX_HOURS, '
        f'             MIN(TIMEDELTA) / (60 * 60)       MIN_HOURS '
        f'      FROM TIMELINES t '
        f'               INNER JOIN ISSUES i ON t.KEY = i.KEY '
        f'      WHERE t.KEY IN '
        f'            (SELECT DISTINCT KEY FROM CHANGELOGS WHERE DATECREATED >= {interval.fromDate()} AND DATECREATED < {interval.toDate()}) '
        f'          AND {where_resolution}'
        f'      GROUP BY 1, 2, 3, 4) '
        f'WHERE '
        f'      AVG_HOUR > 2 '
        f'      AND {max_duration} '
        f'ORDER BY 1; '
    )
    # print(sql)
    df = sw.fetch_df(sql)
    df["AVG_HOUR"] = df["AVG_HOUR"].map(lambda x: np.nan if x is None else float(x))
    df["AVG_DAYS"] = df["AVG_DAYS"].map(lambda x: np.nan if x is None else float(x))
    df.insert(2, "MERGED_STATUS", map_statuses(df["STATUS"]))
    return df


def get_tickets_by_status(sw: SnowflakeWrapper, interval: Interval, use_cached: bool = False) -> dict:
    fname = f"{data_root}/tickets/by_status/tickets-{interval.fname()}.pkl"
    if use_cached and os.path.isfile(fname):
        with open(fname, 'rb') as file:
            return pickle.load(file, encoding='utf8')
    else:
        statuses = {s: DataFrame() for s in get_distinct_statuses(sw)}
        all_data = get_tickets(sw, interval)
        for status in statuses.keys():
            print(f"Working on {status}...")
            statuses[status] = all_data[all_data['STATUS'] == status].set_index("TICKET_KEY")
        with open(fname, "wb") as out_file:
            pickle.dump(statuses, out_file)
    return statuses


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        # interval = Interval(date(2019, 10, 1), date(2020, 1, 1))
        # data = get_tickets(sw, interval)
        # data = get_ticket_counts(sw, breakdowns=["issuePriority", "resolved"])
        priorities = get_ticket_priorities(sw)
        types = get_ticket_types(sw)
