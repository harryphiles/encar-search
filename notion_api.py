import asyncio
import json
import aiohttp
from typing import List, Dict, Any


def generate_payload_create_db(parent_page_id, db_title):
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
    parent_db_id: str, car_id: str, car_data: dict[str, any]
):
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


async def _create_notion_db(session, api_key: str, parent_page_id, db_name):
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
):
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
    api_key: str, parent_page_id: str, db_name: str, car_data: dict[str, dict[str, any]]
):
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


async def _query_notion_db(session, api_key: str, db_id: str, filters: list = None):
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

    async def __get_paginated_requests(
        session, url, header, payload, memo, start_cursor=None
    ):
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

    status, memo = await __get_paginated_requests(session, url, header, payload, memo)

    if status != 200:
        return status
    return memo


async def _update_notion_page(
    session, api_key: str, page_id: str, target_property: list
):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    header = {
        "authorization": api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
    }
    property, property_value = target_property
    payload_format = {
        "Availability": {"select": {"name": property_value}},
        "Insurance & Inspection Check": {"select": {"name": property_value}},
    }
    if property in payload_format:
        payload = {"properties": {property: payload_format.get(property)}}
        async with session.patch(url, headers=header, json=payload) as r:
            if r.status == 200:
                return r.status
            return await r.text()
    return "Specified property is not allowed."


def get_car_ids_from_db(notion_db: List[Dict[str, Any]]) -> Dict[str, bool]:
    """extract car ids from db and return a dict of car id as key and its availability (bool) as value"""
    converter = {
        "✅True": True,
        "🚫False": False,
    }
    car_ids = {
        car.get("properties")
        .get("Car ID")
        .get("title")[0]
        .get("plain_text"): converter.get(
            car.get("properties", {})
            .get("Availability", {})
            .get("select", {})
            .get("name")
        )
        for car in notion_db
        if car.get("properties", {}).get("Car ID", {}).get("title")
    }
    return car_ids


def get_page_ids_from_car_ids(
    notion_db: List[Dict[Any]], car_ids: List[str]
) -> List[str]:
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
    api_key: str, page_ids: list, target_property: list
) -> List[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [
            _update_notion_page(session, api_key, id, target_property)
            for id in page_ids
        ]
        results = await asyncio.gather(*tasks)
        return results


async def get_notion_db(
    api_key: str, db_id: str, filters: list = None
) -> Dict[str, bool]:
    async with aiohttp.ClientSession() as session:
        db = await _query_notion_db(
            session=session, api_key=api_key, db_id=db_id, filters=filters
        )
        return db
