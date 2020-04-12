import json
from datetime import datetime
from typing import Union

import pandas as pd

from src.compute.changelogs import work_activity_on_interval
from src.compute.utils import Interval
from src.db.utils import SnowflakeWrapper


def build_issue_timelines(sw: SnowflakeWrapper, interval: Interval, keys: Union[None, list] = None) -> pd.DataFrame:
    def filter_log_items(items: dict, prop: str) -> Union[dict, None]:
        filtered = [x for x in items["changelogItems"] if x["field"] == prop]
        if len(filtered) > 1:
            raise Exception(f"More than 1 filtered item of type {prop}: {filtered}")
        elif len(filtered) == 1:
            return filtered[0]
        else:
            return None
    changelogs = work_activity_on_interval(sw, interval, keys)
    timelines = []
    for _, row in changelogs.iterrows():
        timeline = []
        date_issue_created = row["DATECREATED"]
        status_from, status_to, changed_assignee = "BACKLOG", "BACKLOG", False  # default status at the beginning (top of workflow)
        assign_from, assign_to, changed_status = None, None, False
        last_change = date_issue_created
        for logs in row["CHANGELOGITEMS"]:
            date_created = logs["dateCreated"]
            change_assignee = filter_log_items(logs, "assignee")
            if change_assignee is not None:
                assign_from = None if "from" not in change_assignee else change_assignee["from"]
                assign_to = None if "to" not in change_assignee else change_assignee["to"]  # the latest assignee
                # print(f"[{author}]: [{status_to}] Reassign from `{assign_from}`, to `{assign_to}`")
                changed_assignee = True
            change_status = filter_log_items(logs, "status")
            if change_status is not None:
                status_from = None if "fromString" not in change_status else change_status["fromString"]
                status_to = None if "toString" not in change_status else change_status["toString"]  # the latest status
                # print(f"[{author}]: [{assign_to}] Transition from `{status_from}`, to `{status_to}`")
                changed_status = True
            if last_change > date_created:
                raise Exception("WAIT A SEC")
            # BUGGY AF :(
            if changed_status and changed_assignee:  # doesn't work if there are 2+ changes in a small delta time
                timeline.append({
                    "status": status_from,
                    "assignee": assign_from,
                    "date_from": last_change,
                    "date_to": date_created,
                    "tdelta": date_created - last_change
                })
                changed_status = False
                changed_assignee = False
            if changed_status:
                timeline.append({
                    "status": status_from,
                    "assignee": assign_to,
                    "date_from": last_change,
                    "date_to": date_created,
                    "tdelta": date_created - last_change
                })
                changed_status = False
            if changed_assignee:
                timeline.append({
                    "status": status_to,
                    "assignee": assign_from,
                    "date_from": last_change,
                    "date_to": date_created,
                    "tdelta": date_created - last_change
                })
                changed_assignee = False
            last_change = date_created
        timeline.append({
            "status": status_to,  # last known status
            "assignee": assign_to,  # last known assignee
            "date_to": None,  # it's still ongoing
            "date_from": last_change,
            "tdelta": datetime.now() - last_change  # TODO HANDLE THIS CASE
        })
        timelines.append(json.dumps(timeline, default=Interval.isDate))
    copy = changelogs.copy()
    copy = copy.drop(["CHANGELOGITEMS"], axis=1)
    # print(timelines)
    copy["timelines"] = timelines
    return copy


def persist_issue_timelines(sw: SnowflakeWrapper, interval: Interval, keys: Union[None, list] = None) -> None:
    try:
        timelines = build_issue_timelines(sw, interval, keys)
        SnowflakeWrapper.execute_df_query(timelines, "timelines_temp", ifexists='replace')
        sw.execute(
            f"CREATE OR REPLACE TABLE TIMELINES ( "
            f"    KEY	    VARCHAR(32),  "
            f"    STATUS	VARCHAR(256),  "
            f"    ASSIGNEE	VARCHAR(128), "
            f"    DATEFROM	TIMESTAMP_NTZ(9),  "
            f"    DATETO	TIMESTAMP_NTZ(9),  "
            f"    TIMEDELTA	NUMBER(38,0) "
            f"); "
        )
        sw.execute(
            f"INSERT ALL INTO TIMELINES "
            f"    SELECT "
            f"        c.KEY key, "
            f"        t.VALUE:status::string status, "
            f"        t.VALUE:assignee::string assignee, "
            f"        TO_TIMESTAMP_NTZ(t.VALUE:date_from) datefrom, "
            f"        TO_TIMESTAMP_NTZ(t.VALUE:date_to) dateto, "
            f"        t.VALUE:tdelta::NUMBER timedelta "
            f"    FROM "
            f"        timelines_temp c, "
            f"        LATERAL FLATTEN(PARSE_JSON(TIMELINES)) t; "
        )
    except Exception as e:
        print(f"Failed persisting timlines to Snowflake: {e}")
        raise e


def get_avg_timeline(sw: SnowflakeWrapper, interval: Interval) -> pd.DataFrame:
    query = (
        f'SELECT '
        f'    STATUS                          "Status", '
        f'    COUNT(DISTINCT KEY)             "UniqueIssues", '
        f'    COUNT(*)                        "Issues", '
        f'    "Issues" - "UniqueIssues"       "Reassignments", '
        f'    AVG(TIMEDELTA) / (60 * 60 * 24) "AvgDays", '
        f'    MAX(TIMEDELTA) / (60 * 60 * 24) "MaxDays", '
        f'    MIN(TIMEDELTA) / (60 * 60 * 24) "MinDays" '
        f'FROM TIMELINES t '
        f'WHERE '
        f'    t.DATEFROM >= {interval.fromDate()} '
        f'    AND t.DATETO < {interval.toDate()} '
        f'GROUP BY 1 '
        f'ORDER BY 1, 4 DESC; '
    )
    print(query)
    return sw.fetch_df(query)


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        # timelines = build_issue_timelines(sw, Interval(date(2019, 7, 1), date(2020, 1, 1)))
        # print(timelines)
        # SnowflakeWrapper.execute_df_query(timelines, "timelines", ifexists='replace')
        # persist_issue_timelines(sw, Interval(date(2019, 7, 1), date(2020, 1, 1)))
