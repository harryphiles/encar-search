from datetime import datetime, timezone, timedelta
import json
from typing import Union, Iterable, Any
from collections import defaultdict


class Expiration:
    def __init__(self, expiration_days: int = 14) -> None:
        self.now = datetime.now(tz=timezone.utc)
        self.expiration_days = expiration_days
        self.iso_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    def _is_expired(self, vehicle_data: dict[str, bool | str]):
        last_edited_time_str = vehicle_data.get("last_edited_time")
        last_edited_time = datetime.strptime(last_edited_time_str, self.iso_format)
        return self.now - last_edited_time > timedelta(days=self.expiration_days)

    def collect_expired(
        self, targets: list[str], vehicle_db: dict[str, dict[str, bool | str]]
    ) -> list[str]:
        """Returns a list of vehicle page IDs which are over user-defined expiration days"""
        return [
            car_id for car_id in targets if self._is_expired(vehicle_db.get(car_id))
        ]


def identify_differences(db: list, encar: list) -> tuple[list[str]]:
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


def find_ids_by_status(
    targets: list[str], reference: dict[str, dict[str, bool | str]], status: bool
) -> tuple[list[str], list[str]]:
    """Returns two lists of car IDs: those that meet the condition and those that do not."""
    matched = []
    unmatched = []
    for t in targets:
        if reference.get(t, {}).get("availability") == status:
            matched.append(t)
        else:
            unmatched.append(t)
    return matched, unmatched


def check_updates_for_intersection(
    intersection: Iterable[str], api_data: dict[str, Any], db_data: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """
    Identify necessary updates for intersection data by comparing API and DB.
    1. Check if availability has changed.
    2. Check price changes.
    """
    changes = defaultdict(dict)
    for car_id in intersection:
        page_id = db_data.get(car_id, {}).get("page_id")
        db_availability = db_data.get(car_id, {}).get("availability")
        # Car id found in the intersection indicates it's available in Encar
        # therefore mark it available in DB
        if not db_availability:
            changes[page_id]["availability"] = "✅True"
        # Get prices from api and db
        api_price_raw = api_data.get(car_id, {}).get("Price")
        db_price_raw = db_data.get(car_id, {}).get("price")
        api_price = int(api_price_raw) if api_price_raw is not None else None
        db_price = (
            int(db_price_raw / 10_000) if db_price_raw is not None else None
        )  # "number": 19500000
        # If prices are not same, update price and comment
        if api_price and db_price and api_price != db_price:
            changes[page_id]["price"] = api_price * 10_000
            db_comment = db_data.get(car_id, {}).get("comment")
            changes[page_id]["comment"] = (
                db_comment + f"→{api_price}"
                if db_comment
                else f"{db_price}→{api_price}"
            )
    return dict(changes) if changes else {}


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


if __name__ == "__main__":
    test_intersection = {"1234", "4321", "2345"}
    test_api_data = {
        "1234": {"Price": 2900,},
        "4321": {"Price": 3100,},
        "2345": {"Price": 3100,},
    }
    test_db_data = {
        "1234": {"price": 31_000_000, "availability": True, "comment": "3100→3000", "page_id": "1"},
        "4321": {"price": 31_000_000, "availability": True, "comment": None, "page_id": "22"},
        "2345": {"price": 31_000_000, "availability": False, "comment": None, "page_id": "333"},
    }
    test = check_updates_for_intersection(
        intersection=test_intersection,
        api_data=test_api_data,
        db_data=test_db_data,
    )
    print(f"{test = }")
