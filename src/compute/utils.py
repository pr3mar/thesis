import json
from datetime import date, datetime, timedelta
from typing import Union

import pandas as pd

import src.config as config
from src.db.utils import SnowflakeWrapper

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
__data_schema = {el["name"].lower().replace(" ", "_"): el["key"] for el in
                 json.loads(open(f"{config.data_root}/schema.json", encoding='utf-8').read())}


class Interval:
    timestamp_format = "%Y-%m-%d %H:%M:%S.%f"
    date_format = "%Y-%m-%d"

    def __init__(self, fromDate: date, toDate: date):
        self.__fromDate = fromDate
        self.__toDate = toDate
        self.validate()

    def __str__(self):
        return f"({self.fromDate()}, {self.toDate()})"

    def fname(self):
        return f"{self.fromDate(escape=False)}_{self.toDate(escape=False)}"

    def fromDate(self, escape=True, raw=False) -> Union[str, date]:
        if raw:
            return self.__fromDate
        return Interval.strdate(self.__fromDate, escape)

    def toDate(self, escape=True, raw=False) -> Union[str, date]:
        if raw:
            return self.__toDate
        return Interval.strdate(self.__toDate, escape)

    def validate(self) -> bool:
        if self.__fromDate <= self.__toDate:
            return True
        else:
            raise RuntimeError(f"Invalid interval given: {self}")

    @staticmethod
    def strdate(d: date, escape=True) -> str:
        if escape:
            return f"'{d.strftime(Interval.date_format)}'"
        return f"{d.strftime(Interval.date_format)}"

    @staticmethod
    def strtimestamp(d: datetime, escape=True) -> str:
        if escape:
            return f"'{d.strftime(Interval.timestamp_format)}'"
        return f"'{d.strftime(Interval.timestamp_format)}'"

    @staticmethod
    def to_datetime(d: date) -> datetime:
        return datetime(d.year, d.month, d.day)

    @staticmethod
    def isDate(d):
        if isinstance(d, datetime):
            return d.strftime(Interval.timestamp_format)
        elif isinstance(d, date):
            return d.strftime(Interval.date_format)
        elif isinstance(d, timedelta):
            return d.total_seconds()


def convert_date(field):
    return f'convert_timezone(\'UTC\', to_timestamp_tz({field}::string, \'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM\'))'


def decode(field: str) -> str:
    return __data_schema[field]


def decode_field(parent_field: str, user_field: str) -> str:
    return f"{parent_field}:{__data_schema[user_field]}"


def decode_user(parent_field: str, user_field: str) -> str:
    return f"{parent_field}:{__data_schema[user_field]}:key::string"


def mask_props(props: list) -> str:
    return ','.join([f'{p[0]} "{p[1]}"' for p in props])


def mask_in(ids: list) -> str:
    return ','.join([f"'{i}'" for i in ids])


def load_with_datetime(pairs):
    """Load with dates"""
    d = {}
    for k, v in pairs:
        if isinstance(v, str):
            try:
                d[k] = datetime.strptime(v, Interval.timestamp_format)
            except ValueError:
                d[k] = v
        else:
            d[k] = v
    return d


def get_distinct_statuses(sw: SnowflakeWrapper) -> list:
    return sw.fetch_df(
        "SELECT ID "
        "FROM STATUSES "
        "ORDER BY 1;"
    )['ID'].tolist()


statuses_of_interest = [
    # "BACKLOG",
    "Ready for dev",
    "Development",
    "Code review",
    "Needs CR fixes",
    "CR On Hold",
    "Testing",
    "Needs QA fixes",
    "Testing On Hold",
]


def map_statuses(statuses: pd.Series) -> pd.Series:
    file_statuses = open(f"{config.data_root}/statuses/all.json")
    status_map = json.load(file_statuses)
    return statuses.map(status_map)


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        # res_keys = cards_active_on_interval(sw,
        #                                     Interval(date(2019, 10, 1), date(2020, 1, 1)),
        #                                     cols=[
        #                                         ('key', 'key'),
        #                                         (decode_user("fields", "owner"), "owner"),
        #                                         (decode_user("fields", "assignee"), "assignee"),
        #                                         (decode_user("fields", "creator"), "creator"),
        #                                         (decode_field("fields", "due_date"), "due_date"),
        #                                     ]
        #                                     )
        # print(len(res_keys))
        # res = work_activity_on_interval(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))
        # print(len(res))
    # print(json.dumps(__data_schema, indent=4))
