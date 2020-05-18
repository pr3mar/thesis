import pandas as pd
from pandas import DataFrame
from src.config import data_root


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
        data_copy.insert(0, item, column_data[item])
    return data_copy


def preprocess_data(fdir: str, input_name: str, output_name: str) -> DataFrame:
    data = pd.read_csv(f'{fdir}/{input_name}')
    hot_encode_columns = [('COMPONENTS', 50), ('LABELS', 50), ('ISSUETYPE', 0), ('ISSUEPRIORITY', 0)]
    for col, min_entry in hot_encode_columns:
        data = hot_encode(data, col, min_entry)
    data.to_csv(f'{fdir}/{output_name}')
    return data


if __name__ == '__main__':
    fdir = f'{data_root}/prediction_data'
    for mode in ['dev', 'ticket']:
        input_name = f'raw_model_data_{mode}-cycling.csv'
        output_name = f'hot_encoded_model_data_{mode}-cycling.csv'
        data = preprocess_data(fdir, input_name, output_name)
