import asyncio
import aiohttp
from bs4 import BeautifulSoup


def check_conditions(car_history, **conditions):
    if car_history == "조회불가차량" or car_history is None:
        return False
    lambdas = {
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
    }
    allowed_keys = [
        "general",
        "business_use",
        "plate_and_owner",
        "irreparable",
        "self_damage",
        "third_party_damage",
        "plate_changed",
        "owner_changed",
    ]
    disallowed = [key for key in conditions.keys() if key not in allowed_keys]
    if disallowed:
        return "Wrong key(s) for condition check"
    for k, v in conditions.items():
        operator, condition = v
        if operator not in lambdas:
            return "Wrong operator(s)"
        if not lambdas[operator](car_history[k], condition):
            return False
    return True


def parse_insurance_data(html):
    soup = BeautifulSoup(html, "html.parser")
    try:
        summary = (
            soup.find("div", class_="rreport")
            .find("div", class_="summary")
            .find_all("td")
        )
        if summary is None:
            return "No data"

        titles = [
            "general",
            "business_use",
            "plate_and_owner",
            "irreparable",
            "self_damage",
            "third_party_damage",
        ]
        values = [i.text.strip() for i in summary if i.text]
        combined = dict(zip(titles, values))

        plate_changed, owner_changed = (
            combined.pop("plate_and_owner").strip().replace("회", "").split("/")
        )
        combined.update(
            {"plate_changed": int(plate_changed), "owner_changed": int(owner_changed)}
        )
        return combined
    except Exception as exc:
        return exc.__class__


async def fetch_insurance_data_async(session, header: dict, car_id):
    history_url = f"http://www.encar.com/dc/dc_cardetailview.do?method=kidiFirstPop&carid={car_id}"
    async with session.get(history_url, headers=header) as response:
        if response.status != 200:
            raise ValueError(f"Failed to fetch data for car ID {car_id}")
        html_text = await response.text()
        return await parse_insurance_data(html_text)


async def check_insurance(header: dict, car_ids: dict, conditions: dict):
    async with aiohttp.ClientSession() as session:
        tasks1 = [fetch_insurance_data_async(session, header, id) for id in car_ids]
        result = await asyncio.gather(*tasks1)
        tasks2 = [
            check_conditions(car_history=history, conditions=conditions)
            for history in result
        ]
        result2 = await asyncio.gather(*tasks2)
        return result2
