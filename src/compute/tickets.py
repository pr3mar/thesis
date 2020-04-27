import os
import pickle
from datetime import date
from typing import Union

import numpy as np
from pandas import DataFrame

from src.compute.utils import mask_in, Interval, get_distinct_statuses
from src.config import data_root
from src.db.utils import SnowflakeWrapper


def get_tickets(sw: SnowflakeWrapper, interval: Interval, ticket_ids: Union[list, None] = None, resolved: bool = True) -> DataFrame:
    tickets = f'AND t.KEY IN ({mask_in(ticket_ids)})' if ticket_ids is not None else ''
    sql = (
        f"SELECT "
        f"    s.id AS STATUS, "
        f"    t.KEY TICKET_KEY, "
        f'    "UniqueIssues", '
        f'    "Issues", '
        f'    "Reassignments", '
        f"    AVG_DAY, "
        f"    MAX_DAYS, "
        f"    MIN_DAYS "
        f"FROM "
        f"    STATUSES s "
        f"    LEFT JOIN ( "
        f"        SELECT "
        f"            t.KEY, "
        f"            STATUS, "
        f'            COUNT(DISTINCT t.KEY)           "UniqueIssues", '
        f'            COUNT(*)                        "Issues", '
        f'            "Issues" - "UniqueIssues"       "Reassignments", '
        f"            AVG(TIMEDELTA) / (60 * 60 * 24) AVG_DAY, "
        f"            MAX(TIMEDELTA) / (60 * 60 * 24) MAX_DAYS, "
        f"            MIN(TIMEDELTA) / (60 * 60 * 24) MIN_DAYS "
        f"        FROM TIMELINES t "
        f"        INNER JOIN ISSUES i ON t.KEY = i.KEY "
        f"        WHERE "
        f"            DATEFROM >= {interval.fromDate()} "
        f"            AND DATETO < {interval.toDate()} "
        f"            AND i.FIELDS:resolution IS {'NOT' if resolved else ''} NULL "
        f'            {tickets} '
        f"        GROUP BY 1, 2 "
        f"        ORDER BY 1 "
        f"    ) t ON t.STATUS = s.id; "
    )
    # print(sql)
    df = sw.fetch_df(sql)
    df["AVG_DAY"] = df["AVG_DAY"].map(lambda x: np.nan if x is None else float(x))
    return df


def get_tickets_by_status(sw: SnowflakeWrapper, interval: Interval, use_cached: bool = True) -> dict:
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
        interval = Interval(date(2019, 10, 1), date(2020, 1, 1))
        data = get_tickets_by_status(sw, interval)
