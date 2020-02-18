import json
from typing import Union

import pandas as pd
from datetime import date, datetime, timedelta
from src.compute.utils import Interval, work_activity_on_interval
from src.db.utils import SnowflakeWrapper


def build_issue_timelines(sw: SnowflakeWrapper, interval: Interval, keys: Union[None, list] = None) -> pd.DataFrame:
    changelogs = work_activity_on_interval(sw, interval, keys)
    timelines = []
    for _, row in changelogs.iterrows():
        timeline = []
        reporter = row["REPORTER"]
        date_issue_created = row["DATECREATED"]
        status_from, status_to, changed_assignee = "BACKLOG", "BACKLOG", False  # default status at the beginning (top of workflow)
        assign_from, assign_to, changed_status = None, reporter, False
        date_transitioned, date_assigned = date_issue_created, date_issue_created
        for logs in row["CHANGELOGITEMS"]:
            # print(f'Number of items {len(logs["changelogItems"])}')
            # if len(logs["changelogItems"]) > 2:
            #     raise Exception("WHOA THERE")
            author = logs["author"]
            date_created = logs["dateCreated"]
            # TODO: can detect re-assignments here
            for log in [x for x in logs["changelogItems"] if x["field"] == "assignee"]:
                assign_from = None if "from" not in log else log["from"]
                assign_to = None if "to" not in log else log["to"]  # the latest assignee
                # print(f"[{author}]: [{status_to}] Reassign from `{assign_from}`, to `{assign_to}`")
                changed_assignee = True
            for log in [x for x in logs["changelogItems"] if x["field"] == "status"]:
                status_from = None if "fromString" not in log else log["fromString"]
                status_to = None if "toString" not in log else log["toString"]  # the latest status
                # print(f"[{author}]: [{assign_to}] Transition from `{status_from}`, to `{status_to}`")
                changed_status = True
            if changed_status and changed_assignee:  # doesn't work if there are 2+ changes in a small delta time
                timeline.append({
                    "status": status_from,
                    "assignee": assign_from if assign_from is not None else author,
                    "date_to": date_created,
                    "date_from": date_transitioned,
                    "tdelta": date_created - date_transitioned
                })
                date_transitioned = date_created
                changed_status = False
                changed_assignee = False
                continue
            if changed_status:
                timeline.append({
                    "status": status_from,
                    "assignee": assign_to if assign_to is not None else f"{author}",
                    "date_to": date_created,
                    "date_from": date_transitioned,
                    "tdelta": date_created - date_transitioned
                })
                date_transitioned = date_created
                changed_status = False
                continue
            if changed_assignee:
                timeline.append({
                    "status": status_to,
                    "assignee": assign_from if assign_from is not None else f"{author}",
                    "date_to": date_assigned,
                    "date_from": date_created,
                    "tdelta": date_created - date_assigned
                })
                date_assigned = date_created
                changed_assignee = False
        timeline.append({
            "status": status_to,  # last known status
            "assignee": assign_to,  # last known assignee
            "date_to": datetime.now(),  # it's still ongoing
            "date_from": date_transitioned,
            "tdelta": datetime.now() - max(date_transitioned, date_assigned)
        })
        timelines.append(json.dumps(timeline, default=Interval.isDate))
    copy = changelogs.copy()
    copy = copy.drop(["CHANGELOGITEMS"], axis=1)
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


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        timelines = build_issue_timelines(sw, Interval(date(2019, 7, 1), date(2020, 1, 1)))
        # print(timelines)
        # SnowflakeWrapper.execute_df_query(timelines, "timelines", ifexists='replace')
        # persist_issue_timelines(sw, Interval(date(2019, 7, 1), date(2020, 1, 1)))
