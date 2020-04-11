import json
from copy import deepcopy
from datetime import timedelta
from typing import Union

import pandas as pd

import src.compute.utils as utils
from src.compute.utils import Interval, load_with_datetime
from src.db.utils import SnowflakeWrapper


def transition_frequency(
    sw: SnowflakeWrapper,
    interval: Interval,
    limit=10,
    order="DESC",
    by_status=True,
    by_week=False,
    by_issue_type=False,
    by_issue_priority=False,
    get_sql=False
) -> Union[pd.DataFrame, str]:
    limit_query = f"LIMIT {limit}" if 0 < limit < 100 else ""

    breakdown_by_issue_type = f'I.FIELDS:issuetype:name::string "IssueType",' if by_issue_type else ""
    breakdown_by_issue_priority = f'I.FIELDS:priority:name::string "IssuePriority",' if by_issue_priority else ""
    breakdown_by_week = f'YEAR "Year", IFF(MONTH = 12 AND WEEKOFYEAR = 1, WEEKOFYEAR + 52, WEEKOFYEAR) "WeekOfYear",' if by_week else ""
    breakdown_by_status = f'CHANGELOGITEM:fromString::string  || \' -> \' || CHANGELOGITEM:toString::string "Transition",' if by_status else ""

    join_issues = "INNER JOIN ISSUES I on C.KEY = I.KEY " if breakdown_by_issue_type or by_issue_priority else ""

    query_group_by_props = 0 + (2 if by_week else 0) + (1 if by_status else 0) + (1 if by_issue_type else 0) + (1 if by_issue_priority else 0)
    group_by = f"GROUP BY {','.join([str(i + 1) for i in range(query_group_by_props)])}" if query_group_by_props > 0 else ""

    query_order = order if order.upper() in ["ASC", "DESC"] else "DESC"
    if not by_week:
        order_by = f"ORDER BY {query_group_by_props + 1} {query_order}"
    else:
        order_by = f"ORDER BY {','.join([str(i + 1) for i in range(query_group_by_props + 1)])} {query_order}"

    # TODO: add join to weeks table which will yield NULLs wherever there are missing values
    #  later on, the NULLs will be converted to zeros (0).
    sql = (
        f'SELECT '
        f'    {breakdown_by_week} '
        f'    {breakdown_by_issue_type} '
        f'    {breakdown_by_issue_priority} '
        f'    {breakdown_by_status} '
        f'    COUNT(*) "TotalTransitions" '
        f'FROM CHANGELOGS C '
        f'      {join_issues} '
        f'WHERE '
        f'    C.CHANGELOGITEM:field::string = \'status\' '
        f'    AND C.DATECREATED >= {interval.fromDate()} AND C.DATECREATED < {interval.toDate()} '
        f'{group_by} '
        f'{order_by} '
        f'{limit_query}; '
    )
    print(sql)
    if get_sql:
        return sql
    return sw.fetch_df(sql)


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
        return sw.fetch_df(get_keys_sql)["KEY"].tolist()
    else:
        return sw.fetch_df(
            f"SELECT "
            f" {utils.mask_props(cols)} "
            f"FROM "
            f"  ISSUES "
            f" WHERE "
            f"  KEY IN ({get_keys_sql});"
        )


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
    ids = f" c.KEY IN ({utils.mask_in(keys)}) AND "
    changelog = sw.fetch_df(
        f"SELECT "
        f"    c.KEY, "
        f"    i.{utils.decode_user('fields', 'reporter')} reporter, "
        f"    TO_TIMESTAMP_NTZ(i.{utils.decode_field('fields', 'created')}::string, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF TZHTZM') dateCreated, "
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
