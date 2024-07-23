import asyncio
import json
from typing import Any
import aiohttp
from payload_generator import PayloadGenerator, VARIABLE_PROPERTY_NAMES, VARIABLE_TYPES


def generate_payload_create_db(parent_page_id: str, db_title: str) -> dict[str, Any]:
    data = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "icon": {"type": "emoji", "emoji": "🚗"},
        "cover": None,
        "title": [{"type": "text", "text": {"content": db_title, "link": None}}],
        "properties": {
            "Car ID": {"title": {}},
            "Maker": {
                "select": {
                    "options": [
                        {"name": "현대", "color": "blue"},
                        {"name": "기아", "color": "red"},
                    ]
                }
            },
            "Model": {"rich_text": {}},
            "Submodel": {"rich_text": {}},
            "Badge": {"rich_text": {}},
            "Badge Detail": {"rich_text": {}},
            "Transmission": {"rich_text": {}},
            "Fuel Type": {
                "select": {
                    "options": [
                        {"name": "⛽Gasoline", "color": "red"},
                        {"name": "🛢️Diesel", "color": "gray"},
                        {"name": "⚡Electric", "color": "green"},
                        {"name": "⚡Hybrid⛽", "color": "blue"},
                    ]
                }
            },
            "Year": {"number": {}},
            "Form Year": {"number": {}},
            "Mileage": {"number": {}},
            "Price": {"number": {"format": "won"}},
            "Location": {"rich_text": {}},
            "Modified Date": {"date": {}},
            "URL": {"url": {}},
            "Insurance & Inspection Check": {
                "select": {
                    "options": [
                        {"name": "✅True", "color": "green"},
                        {"name": "🚫False", "color": "red"},
                        {"name": "🚧Pending", "color": "yellow"},
                    ]
                }
            },
            "Availability": {
                "select": {
                    "options": [
                        {"name": "✅True", "color": "green"},
                        {"name": "🚫False", "color": "red"},
                    ]
                }
            },
        },
    }
    return data


def generate_payload_create_page(
    parent_db_id: str, car_id: str, car_data: dict[str, Any]
) -> dict[str, Any]:
    fuel_converter = {
        "가솔린": "⛽Gasoline",
        "디젤": "🛢️Diesel",
        "전기": "⚡Electric",
        "가솔린+전기": "⚡Hybrid⛽",
    }
    int_converter = {
        1: "✅True",
        0: "🚫False",
        -1: "🚧Pending",
    }
    format_time = lambda time: "T".join(time.split(" ")[:1])
    format_txt = lambda data: {"rich_text": [{"text": {"content": data}}]}
    data = {
        "parent": {"database_id": parent_db_id},
        "icon": None,
        "cover": None,
        "properties": {
            "Car ID": {"title": [{"text": {"content": car_id}}]},
            "Maker": {"select": {"name": car_data.get("Maker", "")}},
            "Model": format_txt(car_data.get("Model", "")),
            "Submodel": format_txt(car_data.get("Submodel", "")),
            "Badge": format_txt(car_data.get("Badge", "")),
            "Badge Detail": format_txt(car_data.get("BadgeDetail", "")),
            "Transmission": format_txt(car_data.get("Transmission", "")),
            "Fuel Type": {
                "select": {"name": fuel_converter.get(car_data.get("FuelType", 0))}
            },
            "Availability": {
                "select": {"name": int_converter.get(car_data.get("Availability", 0))}
            },
            "Insurance & Inspection Check": {
                "select": {
                    "name": int_converter.get(car_data.get("InsuranceInspection", 0))
                }
            },
            "Year": {"number": car_data.get("Year", 0)},
            "Form Year": {"number": int(car_data.get("FormYear", 0))},
            "Mileage": {"number": car_data.get("Mileage", 0)},
            "Price": {"number": car_data.get("Price", 0) * 10000},
            "Location": format_txt(car_data.get("OfficeCityState", "")),
            "Modified Date": {
                "date": {"start": format_time(car_data.get("ModifiedDate", ""))}
            },
            "URL": {
                "url": f"http://www.encar.com/dc/dc_cardetailview.do?pageid=dc_carsearch&listAdvType=pic&carid={car_id}"
            },
        },
    }
    return data


async def _create_notion_db(
    session, api_key: str, parent_page_id: str, db_name: str
) -> dict[str, Any]:
    url = "https://api.notion.com/v1/databases"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    payload = generate_payload_create_db(parent_page_id, db_name)

    async with session.post(url, headers=header, json=payload) as r:
        if r.status != 200:
            return r.status
        text = await r.text()
        return json.loads(text).get("id")


async def _create_notion_page(
    session, api_key: str, db_id: str, car_id: str, car_data: list
) -> str | int:
    url = "https://api.notion.com/v1/pages"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    payload = generate_payload_create_page(db_id, car_id, car_data)
    async with session.post(url, headers=header, json=payload) as r:
        if r.status == 200:
            return r.status
        return await r.text()


async def create_db_and_pages(
    api_key: str, parent_page_id: str, db_name: str, car_data: dict[str, dict[str, Any]]
) -> list[str | int]:
    async with aiohttp.ClientSession() as session:
        db_id = await _create_notion_db(
            session,
            api_key=api_key,
            parent_page_id=parent_page_id,
            db_name=db_name,
        )
        tasks = [
            _create_notion_page(
                session,
                api_key=api_key,
                db_id=db_id,
                car_id=id,
                car_data=car_data.get(id),
            )
            for id in car_data
        ]
        result = await asyncio.gather(*tasks)
        return result


async def _query_notion_db(
    session, api_key: str, db_id: str, filters: list = None
) -> list[dict[str, Any]]:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    memo = []
    payload = {}
    if filters:
        maker, model, submodel = filters
        payload = {
            "filter": {
                "and": [
                    {"property": "Maker", "select": {"equals": maker}},
                    {"property": "Model", "rich_text": {"equals": model}},
                    {"property": "Submodel", "rich_text": {"equals": submodel}},
                ]
            }
        }

    async def __get_paginated_requests(start_cursor: str = None):
        while True:
            if start_cursor:
                payload["start_cursor"] = start_cursor
            async with session.post(url, headers=header, json=payload) as r:
                if r.status != 200:
                    return r.status, memo
                json_data = await r.json()
                has_more = json_data.get("has_more")
                results = json_data.get("results")
                next_cursor = json_data.get("next_cursor")
                memo.extend(results)
                if not has_more:
                    break
                start_cursor = next_cursor
        return 200, memo

    status, memo = await __get_paginated_requests()

    if status != 200:
        return status
    return memo


async def _update_notion_page(
    session, api_key: str, page_id: str, update_data: dict[str, Any]
) -> str | int:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    if update_data:
        payload = {"properties": update_data}
        async with session.patch(url, headers=header, json=payload) as r:
            if r.status == 200:
                return r.status
            return await r.text()
    return "Data for update is not provided."


async def _trash_notion_page(session, api_key: str, page_id: str) -> str | int:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    payload = {"in_trash": True}
    async with session.patch(url, headers=header, json=payload) as r:
        if r.status == 200:
            return r.status
        return await r.text()


def extract_specific_data(notion_db: list[dict[str, Any]]) -> dict[str, dict]:
    """Return a dict of car IDs as keys, availability and last_edited_time as values."""
    converter = {
        "✅True": True,
        "🚫False": False,
    }
    return {
        car.get("properties")
        .get("Car ID")
        .get("title")[0]
        .get("plain_text"): {
            "availability": converter.get(
                car.get("properties", {})
                .get("Availability", {})
                .get("select", {})
                .get("name")
            ),
            "page_id": car.get("id"),
            "last_edited_time": car.get("last_edited_time"),
            "price": car.get("properties", {}).get("Price", {}).get("number"),
            "comment": (
                car.get("properties", {})
                .get("Comment", {})
                .get("rich_text", [])[0]
                .get("text", {})
                .get("content")
                if car.get("properties", {}).get("Comment", {}).get("rich_text", [])
                else None
            ),
        }
        for car in notion_db
        if car.get("properties", {}).get("Car ID", {}).get("title")
    }


def get_page_ids_from_formatted_db(targets: list[str], formatted_db: dict[str, dict[str, bool | str]]) -> list[str]:
    return [formatted_db.get(car_id, {}).get("page_id") for car_id in targets]


def get_page_ids_from_car_ids(
    notion_db: list[dict[str, Any]], car_ids: list[str]
) -> list[str]:
    page_ids = []
    for car in notion_db:
        car_id_title = car.get("properties", {}).get("Car ID", {}).get("title", [])
        if not car_id_title:
            continue
        car_id = car_id_title[0].get("plain_text")
        if car_id in car_ids:
            page_ids.append(car.get("id"))
    return page_ids


async def call_create_notion_db(api_key: str, parent_page_id: str, db_name: str):
    async with aiohttp.ClientSession() as session:
        result = await _create_notion_db(
            session=session,
            api_key=api_key,
            parent_page_id=parent_page_id,
            db_name=db_name,
        )
        return result


async def create_notion_pages(
    api_key: str, db_id: str, car_data: dict[str, dict[str, any]]
) -> list[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            _create_notion_page(
                session,
                api_key=api_key,
                db_id=db_id,
                car_id=id,
                car_data=car_data.get(id),
            )
            for id in car_data
        ]
        result = await asyncio.gather(*tasks)
        return result


async def update_pages_with_page_ids(
    api_key: str, page_ids: list, update_data: dict[str, Any]
) -> list[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            _update_notion_page(session, api_key, id, update_data) for id in page_ids
        ]
        results = await asyncio.gather(*tasks)
        return results


async def update_pages_with_update_targets(
    api_key: str, update_targets: dict[str, dict[str, Any]]
) -> list[str]:
    pg = PayloadGenerator(VARIABLE_PROPERTY_NAMES, VARIABLE_TYPES)
    async with aiohttp.ClientSession() as session:
        tasks = [
            _update_notion_page(session, api_key, page_id, pg.generate(update_data))
            for page_id, update_data in update_targets.items()
        ]
        results = await asyncio.gather(*tasks)
        return results


async def trash_pages_with_page_ids(api_key: str, page_ids: list) -> list[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [_trash_notion_page(session, api_key, id) for id in page_ids]
        results = await asyncio.gather(*tasks)
        return results


async def get_notion_db(
    api_key: str, db_id: str, filters: list = None
) -> dict[str, bool]:
    async with aiohttp.ClientSession() as session:
        db = await _query_notion_db(
            session=session, api_key=api_key, db_id=db_id, filters=filters
        )
        return db
