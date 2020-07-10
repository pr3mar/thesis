import json
from typing import Union

import pandas as pd
from pandas import DataFrame
from src.config import data_root

priority_order = {
    "Blocker": 4,
    "Critical": 3,
    "Major": 2,
    "Minor": 1,
    "Trivial": 0,
}

type_order = {
    "Bug": 0,
    "Bug (Sub-task)": 1,
    "Epic": 2,
    "Improvement (Sub-task)": 3,
    "Internal Improvement": 4,
    "New Feature or Improvement": 5,
    "Prototype": 6,
    "Sub-task": 7
}


def enumerate_vals(data: DataFrame, column):
    copy = data.copy()
    vals = {val: i for i, val in enumerate(set(data[column]))}
    print(json.dumps(vals, indent=2))
    copy[column] = copy[column].map(vals)
    return copy


def mergeAndOmitColumnValues(data: DataFrame, column: str, metaData: dict) -> DataFrame:
    copy = data.copy()
    copy = copy.loc[~copy[column].isin(metaData["omit"])]
    copy[column] = copy[column].apply(lambda x: metaData["merge"][x] if x in metaData["merge"] else x)
    return copy


def get_columns_to_drop(hot_encoded, min_entries):
    data_sum = hot_encoded.sum()
    return list(data_sum.loc[data_sum < min_entries].index)


def hot_encode(data: DataFrame, col: str, min_entries: int) -> DataFrame:
    data_copy = data.copy()
    hot_encoded = pd.get_dummies(data[col])
    columns_to_drop = get_columns_to_drop(hot_encoded, min_entries)
    column_data = hot_encoded.drop(columns=columns_to_drop)
    data_copy = data_copy.drop(columns=[col])
    for item in list(column_data):
        data_copy.insert(1, item, column_data[item])
    return data_copy


def preprocess_ticket_data(fdir: str, input_name: str, output_name: str, drop: Union[list, None] = None) -> DataFrame:
    if drop is None:
        drop = list()
    data = pd.read_csv(f'{fdir}/{input_name}')
    labels_metaData = json.load(open(f"{data_root}/merge/labels.json"))
    components_metaData = json.load(open(f"{data_root}/merge/components.json"))

    data = data.drop(columns=drop)

    data = mergeAndOmitColumnValues(data, "LABEL", labels_metaData)
    data = mergeAndOmitColumnValues(data, "COMPONENT", components_metaData)

    for column, ordering in [('ISSUETYPE', type_order), ('ISSUEPRIORITY', priority_order)]:
        data[column] = data[column].map(ordering)

    for col, min_entry in [('COMPONENT', 50), ('LABEL', 50)]:
        data = hot_encode(data, col, min_entry)

    # NOTE: we calculate the mean of the following values because they are the duplicated in the provided raw data
    # with this change we get 1 row == 1 ticket, thus the lower number of rows in the output
    # Additionally, we are filtering out rows which are associated with a low number of breakdown_labels or components,
    # as they are insignificant and cause noise in the data.
    mean_cols = ["NUMBEROFCOMPONENTS", "NUMBEROFLABELS", "NUMBEROFCOMMENTS", "NUMBEROFLINKEDISSUES", "DEGREEOFCYCLING", "DAYSINDEVELOPMENT"]
    agg_cols = {col: ('mean' if col in mean_cols else 'min') for col in list(data)}
    del agg_cols["TICKETKEY"]

    data = data.groupby("TICKETKEY").agg(agg_cols).reset_index().drop(columns=["TICKETKEY"])

    data.to_csv(f'{fdir}/{output_name}', index=False)
    return data


def preprocess_dev_data(fdir: str, input_name: str, output_name: str, drop=None) -> DataFrame:
    if drop is None:
        drop = list()
    data = pd.read_csv(f'{fdir}/{input_name}')
    labels_metaData = json.load(open(f"{data_root}/merge/labels.json"))
    data = mergeAndOmitColumnValues(data, "LABEL", labels_metaData)

    data = data.drop(columns=drop)

    for col, min_entry in [('LABEL', 50), ('COMPONENT', 50)]:
        data = hot_encode(data, col, min_entry)

    mean_cols = ["AUTHOREDACTIVITY", "NUMBEROFLABELS", "NUMBEROFCOMMENTS", "DEGREEOFCYCLING", "DAYSINDEVELOPMENT"]
    agg_cols = {col: ('mean' if col in mean_cols else 'min') for col in list(data)}
    del agg_cols["TICKETKEY"]
    del agg_cols["DEVELOPER"]
    data = data.groupby(by=["TICKETKEY", "DEVELOPER"]).agg(agg_cols).reset_index().drop(columns=["TICKETKEY"])

    v = data[['DEVELOPER']]
    data = data[v.replace(v.apply(pd.Series.value_counts)).gt(200).all(1)]

    for col in ['ISSUETYPE', 'ISSUEPRIORITY', "DEVELOPER"]:
        data = enumerate_vals(data, col)

    dev_data = data["DEVELOPER"]
    data = data.drop(columns=["DEVELOPER"])
    data["DEVELOPER"] = dev_data

    data.to_csv(f'{fdir}/{output_name}', index=False)
    return data


if __name__ == '__main__':
    ticket_fdir = f'{data_root}/prediction_data/ticket_model'
    modes = [
        "model_data_development_filtered_hours_all",
        "model_data_development_filtered_hours_1-quarter",
        "model_data_development_filtered_hours_1-month",
        "model_data_development_filtered_hours_1-day",
        "model_data_development_filtered_hours_10-days",
        "model_data_development_filtered_hours_10-days",
        "model_data_development_filtered_sum_hours_10-days",
    ]
    for mode in modes:
        print(mode)
        ticket_input_name = f'raw_{mode}.csv'
        fname = f"encoded_{mode}"
        preprocess_ticket_data(ticket_fdir, ticket_input_name, f'{fname}.csv')
        preprocess_ticket_data(ticket_fdir, ticket_input_name, f'{fname}_real-data.csv', drop=["NUMBEROFCOMMENTS", "DEGREEOFCYCLING"])

    # dev_fdir = f'{data_root}/prediction_data/dev_model'
    # dev_input_name = f'raw_model_data_unfiltered.csv'
    # dev_output_name = f'encoded_model_data_unfiltered_real-data.csv'
    # dev_out_data = preprocess_dev_data(dev_fdir, dev_input_name, dev_output_name, drop=["NUMBEROFCOMMENTS", "DEGREEOFCYCLING"])
