import json
from typing import Dict, Tuple, List, Union


def identify_differences(db: list, encar: list) -> Tuple[List[str]]:
    """returns (new, intersection, unavailable) for sorted db and api inputs"""
    db, encar = sorted(db[:]), sorted(encar[:])
    len_db, len_api = len(db), len(encar)
    pointer_db, pointer_api = 0, 0
    new, intersection, unavailable = [], [], []

    while pointer_db < len_db and pointer_api < len_api:
        db_data = db[pointer_db]
        api_data = encar[pointer_api]

        if db_data == api_data:
            intersection.append(db_data)
            pointer_db += 1
            pointer_api += 1
        elif db_data < api_data:
            unavailable.append(db_data)
            pointer_db += 1
        else:
            new.append(api_data)
            pointer_api += 1

    # Add remaining elements from DB
    while pointer_db < len_db:
        unavailable.append(db[pointer_db])
        pointer_db += 1

    # Add remaining elements from API
    while pointer_api < len_api:
        new.append(encar[pointer_api])
        pointer_api += 1

    return new, intersection, unavailable


def find_ids_by_status(targets: list[str], reference: dict[str, bool], status: bool) -> list[str]:
    """Returns a list of car ids from targets that meet availability status in the reference."""
    result = [t for t in targets if reference.get(t) == status]
    return result


def write_file(
    file_name: str, input_text: Union[str, dict, list], file_type: str = "json"
) -> None:
    with open(file_name, "w", encoding="utf-8") as fp:
        if file_type == "json":
            json.dump(input_text, fp, indent=4, ensure_ascii=False)
        else:
            fp.write(input_text)


def write_file_append(
    file_name: str, input_text: Union[str, dict, list], file_type: str = "json"
) -> None:
    with open(file_name, "a", encoding="utf-8") as fp:
        if file_type == "json":
            json.dump(input_text, fp, indent=4, ensure_ascii=False)
        else:
            fp.write(input_text)


def read_file(file_name: str, file_type: str = "json") -> None:
    with open(file_name, "r", encoding="utf-8") as fp:
        if file_type == "json":
            return json.load(fp)
        return fp.read()
