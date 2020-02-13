import json
import pandas as pd
import code.config as config
from datetime import date
from code.db.utils import SnowflakeWrapper

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
__data_schema = {el["name"].lower().replace(" ", "_"): el["key"] for el in
                 json.loads(open(f"{config.data_root}/schema.json", encoding='utf-8').read())}


def decode(field: str) -> str:
    return __data_schema[field]


def decode_field(parent_field: str, user_field: str) -> str:
    return f"{parent_field}:{__data_schema[user_field]}"


def decode_user(parent_field: str, user_field: str) -> str:
    return f"{parent_field}:{__data_schema[user_field]}:key::string"


def strdate(d: date) -> str:
    return d.strftime('%Y-%m-%d')


def validate_interval(interval: (date, date)) -> bool:
    if interval[0] <= interval[1]:
        return True
    else:
        raise RuntimeError(f"Invalid interval given: {interval}")


def mask_props(props: list) -> str:
    return ','.join([f'{p[0]} "{p[1]}"' for p in props])


def changelog_on_interval(sw: SnowflakeWrapper, interval: (date, date)):
    validate_interval(interval)
    return sw.execute_query(
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
        f"    DATECREATED >= '{strdate(interval[0])}' AND "
        f"    DATECREATED < '{strdate(interval[1])}' "
        f"GROUP BY KEY; "
    )


def cards_active_on_interval(sw: SnowflakeWrapper, interval: (date, date), cols=None):
    validate_interval(interval)
    get_keys_sql = (
        f"SELECT "
        f"  KEY "
        f"FROM "
        f"  CHANGELOGS "
        f"WHERE "
        f"  DATECREATED >= '{strdate(interval[0])}' AND "
        f"  DATECREATED < '{strdate(interval[1])}' "
        f"GROUP BY KEY"
    )
    if cols is None:
        return sw.execute_query(get_keys_sql)
    else:
        sql = (
            f"SELECT "
            f" {mask_props(cols)} "
            f"FROM "
            f"  ISSUES "
            f" WHERE "
            f"  KEY IN ({get_keys_sql});"
        )
        print(sql)
        return sw.execute_query(sql)


if __name__ == '__main__':
    with SnowflakeWrapper.create_snowflake_connection() as connection:
        sw = SnowflakeWrapper(connection)
        result = changelog_on_interval(sw, (date(2019, 10, 1), date(2020, 1, 1)))
        print(result)
        result = cards_active_on_interval(sw,
                                          (date(2019, 10, 1), date(2020, 1, 1)),
                                          cols=[
                                              ('key', 'key'),
                                              (decode_user("fields", "owner"), "owner"),
                                              (decode_user("fields", "assignee"), "assignee"),
                                              (decode_user("fields", "creator"), "creator"),
                                              (decode_field("fields", "due_date"), "due_date"),
                                          ]
                                          )
        print(len(result))
        print(result)
    # print(json.dumps(__data_schema, indent=4))
