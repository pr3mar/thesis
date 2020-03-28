import json
from src.config import data_root


def reverse_list(data: dict) -> dict:
    reversed = {}
    for k, v in data.items():
        if v not in reversed:
            reversed[v] = [k]
        else:
            reversed[v].append(k)
    return reversed


if __name__ == '__main__':
    with open(f"{data_root}/statuses/all.json") as in_file, open(f"{data_root}/statuses/merged.json", "w") as out_file:
        data = json.load(in_file)
        reversed_list = reverse_list(data)
        print(json.dumps(reversed_list, indent=2))
        json.dump(reversed_list, out_file)

