import json


def read_json(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf8") as read_file:
        return json.load(read_file)
