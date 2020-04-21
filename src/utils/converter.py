import pickle
from pathlib import Path
from src.config import data_root


def to_csv(obj: dict, output_dir: str) -> None:
    Path(f"{output_dir}").mkdir(parents=True, exist_ok=True)
    for status, df in obj.items():
        dump = df.drop(["USERID"], axis=1)
        dump.to_csv(f"{output_dir}{status.lower().replace(' ', '-')}.csv")


if __name__ == '__main__':
    ofname = f"{data_root}/anonymous/by_status/csv/avg_devs-2019-01-01_2019-04-01/"
    fname = f"{data_root}/by_status/avg_devs-2019-01-01_2019-04-01.pkl"
    data = pickle.load(open(fname, 'rb'), encoding='utf8')
    to_csv(data, ofname)
