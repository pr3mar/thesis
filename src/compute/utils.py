import json
from typing import Union

import pandas as pd
import src.config as config
from datetime import date
from src.db.utils import SnowflakeWrapper

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
__data_schema = {el["name"].lower().replace(" ", "_"): el["key"] for el in
                 json.loads(open(f"{config.data_root}/schema.json", encoding='utf-8').read())}


class Interval:
    def __init__(self, fromDate: date, toDate: date):
        self.__fromDate = fromDate
        self.__toDate = toDate

    def fromDate(self) -> str:
        return Interval.strdate(self.__fromDate)

    def toDate(self) -> str:
        return Interval.strdate(self.__toDate)

    def validate(self) -> bool:
        if self.__fromDate <= self.__toDate:
            return True
        else:
            raise RuntimeError(f"Invalid interval given: ({self.fromDate()}, {self.toDate()})")

    @staticmethod
    def strdate(d: date) -> str:
        return f"'{d.strftime('%Y-%m-%d')}'"


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


def work_activity_on_interval(sw: SnowflakeWrapper, interval: Interval, keys: Union[None, list] = None) -> pd.DataFrame:
    """
    Returns the work activity (changes of assignees and statuses) on a given interval.
    To be able to infer the beginning state of the issue at intervalStartDate, the activity is provided on the interval:
    [issueCreationDate, intervalEndDate)

    :param sw: SnowflakeWrapper
    :param interval: (from:date, to:date)
    :param keys: custom list of cards
    :return:
    """
    interval.validate()
    if keys is None:
        keys = cards_active_on_interval(sw, interval)
    ids = f" KEY IN ({mask_in(keys)}) AND "
    changelog = sw.execute_query(
        f"SELECT "
        f"    KEY, "
        f"    ARRAY_AGG( "
        f"        OBJECT_CONSTRUCT( "
        f"            'author', USERID, "
        f"            'dateCreated', DATECREATED, "
        f"            'changelogItem', CHANGELOGITEM "
        f"            ) "
        f"        ) CHANGELOGITEMS "
        f"FROM CHANGELOGS "
        f"WHERE "
        f"    changelogItem:field IN ('status', 'assignee') AND "
        f"    {ids} "
        f"    DATECREATED < {interval.toDate()} "
        f"GROUP BY KEY; "
    )
    changelog['CHANGELOGITEMS'] = changelog['CHANGELOGITEMS'].apply(lambda x: json.loads(x))
    return changelog


def cards_active_on_interval(sw: SnowflakeWrapper, interval: Interval, cols=None) -> Union[list, pd.DataFrame]:
    interval.validate()
    get_keys_sql = (
        f"SELECT "
        f"  KEY "
        f"FROM "
        f"  CHANGELOGS "
        f"WHERE "
        f"  DATECREATED >= {interval.fromDate()} AND "
        f"  DATECREATED < {interval.toDate()} "
        f"GROUP BY KEY"
    )
    if cols is None:
        return sw.execute_query(get_keys_sql)["KEY"].tolist()
    else:
        return sw.execute_query(
            f"SELECT "
            f" {mask_props(cols)} "
            f"FROM "
            f"  ISSUES "
            f" WHERE "
            f"  KEY IN ({get_keys_sql});"
        )


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        res_keys = cards_active_on_interval(sw,
                                            Interval(date(2019, 10, 1), date(2020, 1, 1)),
                                            cols=[
                                                ('key', 'key'),
                                                (decode_user("fields", "owner"), "owner"),
                                                (decode_user("fields", "assignee"), "assignee"),
                                                (decode_user("fields", "creator"), "creator"),
                                                (decode_field("fields", "due_date"), "due_date"),
                                            ]
                                            )
        print(len(res_keys))
        res = work_activity_on_interval(sw, Interval(date(2019, 10, 1), date(2020, 1, 1)))
        print(len(res))
        print(res)
    # print(json.dumps(__data_schema, indent=4))
