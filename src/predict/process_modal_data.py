import json
import pandas as pd
from pandas import DataFrame
from src.config import data_root


def enumerate_vals(data: DataFrame, column):
    copy = data.copy()
    vals = {val: (i + 1) for i, val in enumerate(set(data[column]))}
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


def preprocess_data(fdir: str, input_name: str, output_name: str) -> DataFrame:
    data = pd.read_csv(f'{fdir}/{input_name}')
    labels_metaData = json.load(open(f"{data_root}/merge/labels.json"))
    components_metaData = json.load(open(f"{data_root}/merge/components.json"))

    data = mergeAndOmitColumnValues(data, "LABEL", labels_metaData)
    data = mergeAndOmitColumnValues(data, "COMPONENT", components_metaData)

    for col in ['ISSUETYPE', 'ISSUEPRIORITY']:
        data = enumerate_vals(data, col)

    for col, min_entry in [('COMPONENT', 50), ('LABEL', 50)]:
        data = hot_encode(data, col, min_entry)

    # NOTE: we calculate the mean of the following values because they are the duplicated in the provided raw data
    # with this change we get 1 row == 1 ticket, thus the lower number of rows in the output
    # Additionally, we are filtering out rows which are associated with a low number of labels or components,
    # as they are insignificant and cause noise in the data.
    mean_cols = ["NUMBEROFCOMPONENTS", "NUMBEROFLABELS", "DEGREEOFCYCLING", "TIMETORESOLVE"]
    agg_cols = {col: ('mean' if col in mean_cols else 'min') for col in list(data)}
    del agg_cols["TICKETKEY"]

    data = data.groupby("TICKETKEY").agg(agg_cols).reset_index().drop(columns=["TICKETKEY"])

    data.to_csv(f'{fdir}/{output_name}', index=False)
    return data


if __name__ == '__main__':
    fdir = f'{data_root}/prediction_data'
    input_name = f'raw_model_data_development.csv'
    output_name = f'encoded_model_data_development.csv'
    out_data = preprocess_data(fdir, input_name, output_name)
