import time
import requests
from config import get_delay_seconds


def get_query(maker: str, model: str, submodel: str, **conditions: dict) -> str:
    default = f"(And.Hidden.N._.(C.CarType.Y._.(C.Manufacturer.{maker}._.(C.ModelGroup.{model}._.Model.{submodel}.)))"
    primary_conditions = ["year", "mileage", "price"]
    for cond, cond_data in conditions.items():
        if cond in primary_conditions:
            start = cond_data.get("start", "")
            end = cond_data.get("end", "")
            default += f"_.{cond.title()}.range({start}..{end})."
        elif cond == "options":
            options = [f"_.{cond.title()}.{option}." for option in cond_data]
            default += "".join(options)
    closing = "_.SellType.일반._.Condition.Inspection._.Condition.Record.)"
    return default + closing


def get_encar_vehicle_data(header: dict, query: str, target_vehicle: list) -> dict:
    start = 0
    increase_by = 100
    search_url = "http://api.encar.com/search/car/list/premium?count=true&q="
    sort_range = f"&sr=|PriceDesc|{start}|{increase_by}"
    url = search_url + query + sort_range

    r = requests.get(url, headers=header)
    if r.status_code != 200:
        return r.raise_for_status()
    remaining_cars = r.json().get("Count")
    checked_cars_ids = {}
    duplicate_checks = set()

    while remaining_cars > start:
        sort_range = f"&sr=|PriceDesc|{start}|{increase_by}"
        url = search_url + query + sort_range
        time.sleep(get_delay_seconds())
        r = requests.get(url, headers=header)
        if r.status_code != 200:
            return r.raise_for_status()
        fetched_cars = r.json().get("SearchResults")
        for car in fetched_cars:
            car_id = car.get("Id", "")
            mileage = car.get("Mileage", "")
            price = car.get("Price", "")
            mileage_price = f"{mileage}_{price}"
            
            if mileage_price not in duplicate_checks:
                duplicate_checks.add(mileage_price)
                checked_cars_ids[car_id] = {
                    "Badge": car.get("Badge", ""),
                    "BadgeDetail": car.get("BadgeDetail", ""),
                    "Transmission": car.get("Transmission", ""),
                    "FuelType": car.get("FuelType", ""),
                    "Year": car.get("Year", ""),
                    "FormYear": car.get("FormYear", ""),
                    "Mileage": mileage,
                    "Price": price,
                    "OfficeCityState": car.get("OfficeCityState", ""),
                    "ModifiedDate": car.get("ModifiedDate", ""),
                    "Availability": 1,
                    "InsuranceInspection": -1,
                    "Maker": target_vehicle[0],
                    "Model": target_vehicle[1],
                    "Submodel": target_vehicle[2],
                }
        start += increase_by

    return checked_cars_ids