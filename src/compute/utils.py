import json
from copy import deepcopy
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

    def fromDate(self, escape=True) -> str:
        return Interval.strdate(self.__fromDate, escape)

    def toDate(self, escape=True) -> str:
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
    def isDate(d):
        if isinstance(d, datetime):
            return d.strftime(Interval.timestamp_format)
        elif isinstance(d, date):
            return d.strftime(Interval.date_format)
        elif isinstance(d, timedelta):
            return d.total_seconds()


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
    ids = f" c.KEY IN ({mask_in(keys)}) AND "
    changelog = sw.fetch_df(
        f"SELECT "
        f"    c.KEY, "
        f"    i.{decode_user('fields', 'reporter')} reporter, "
        f"    TO_TIMESTAMP_NTZ(i.{decode_field('fields', 'created')}::string, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF TZHTZM') dateCreated, "
        f"    ARRAY_AGG( "
        f"        OBJECT_CONSTRUCT( "
        f"            'author', USERID, "
        f"            'dateCreated', DATECREATED, "
        f"            'changelogItems', ARRAY_CONSTRUCT(CHANGELOGITEM) "
        f"            ) "
        f"        ) CHANGELOGITEMS "
        f"FROM CHANGELOGS c INNER JOIN ISSUES i ON c.KEY = i.KEY "
        f"WHERE "
        f"    c.changelogItem:field IN ('status', 'assignee') AND "
        f"    {ids} "
        f"    c.DATECREATED < {interval.toDate()} "
        f"GROUP BY 1, 2, 3 "
        # f"    c.KEY IN ('MAB-14432') AND "
        # f"LIMIT 1"gcsa
    )
    changelog['CHANGELOGITEMS'] = changelog['CHANGELOGITEMS'].apply(
        lambda x: sort_and_merge(json.loads(x, object_pairs_hook=load_with_datetime)))
    return changelog


def sort_and_merge(changelog):
    """
    merges reassignments of issues into a single item, conditions:
        - same author
        - time delta between actions < 5 minutes
    :param changelog:
    :return: sorted & merged changelog items
    """
    def changelog_affected_fields(item: dict) -> list:
        # field <=> which field has been affected in the changelog item
        return [i["field"] for i in item["changelogItems"]]

    MAX_DELTA_MINUTES = 5
    chlog = deepcopy(changelog)
    chlog.sort(key=lambda x: x['dateCreated'])
    prev_id = 0
    prev_item = chlog[prev_id]
    current_id = prev_id
    while (current_id + 1) < len(chlog):
        current_id += 1
        current_item = chlog[current_id]
        fields: set = set(changelog_affected_fields(current_item) + changelog_affected_fields(prev_item))
        delta: timedelta = current_item["dateCreated"] - prev_item["dateCreated"]
        # helpful for debugging:
        # print(f"Items [{len(fields)}] = {fields}, prev_created", prev_item["dateCreated"], "curr_created", current_item["dateCreated"], delta)
        # print(f"prev author = {prev_item['author']}, curr author = {current_item['author']}")
        if current_item["author"] != prev_item["author"] or \
                fields != {"assignee", "status"} or len(fields) != 2 or \
                (delta.seconds // 60) > MAX_DELTA_MINUTES \
                or len(prev_item['changelogItems']) > 1:
            prev_id, prev_item = current_id, current_item
            continue
        changelogItems = deepcopy(current_item["changelogItems"]) + deepcopy(prev_item["changelogItems"])
        chlog[prev_id] = {
            "author": current_item["author"],
            "dateCreated": prev_item["dateCreated"],
            "changelogItems": changelogItems
        }
        chlog[current_id]["delete"] = True
        prev_item = chlog[prev_id]
    # print("merged: ", [x for x in chlog if len(x['changelogItems']) > 2])
    filtered = [x for x in chlog if "delete" not in x]
    return filtered


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
