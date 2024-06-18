from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any


@dataclass
class Number:
    number: int


@dataclass
class Content:
    content: str


@dataclass
class SelectItem:
    name: str


@dataclass
class TextItem:
    text: Content


@dataclass
class DateItems:
    start: datetime
    # end: Optional[datetime] = None


@dataclass
class Date:
    date: DateItems


@dataclass
class Title:
    title: str


@dataclass
class Select:
    select: str


@dataclass
class RichText:
    rich_text: list[TextItem]




class PayloadGenerator:
    def __init__(
        self, variable_to_property: dict[str, str], variable_types: dict[str, set[str]]
    ) -> None:
        self.variable_to_property = variable_to_property
        self.variable_types = variable_types

    def _get_variable_type(
        self, types_data: dict[str, set[str]], variable_name: str
    ) -> str | None:
        for type, variables in types_data.items():
            if variable_name in variables:
                return type
        return None

    def _get_payload(self, format_type: str, input_data: str):
        formats = {
            "rich_text": lambda x: RichText([TextItem(Content(x))]),
            "title": lambda x: Title([TextItem(Content(x))]),
            "select": lambda x: Select(SelectItem(x)),
            "number": lambda x: Number(x),
            "date": lambda x: Date(DateItems(x.isoformat())),
        }
        if format_type in formats:
            return asdict(formats.get(format_type)(input_data))
        return None
        
    def generate_properties_payload(
        self, properties_data: dict[str, str | int], filter_keys: set[str] | None = None
    ) -> dict:
        def should_include(property: str) -> bool:
            if not filter_keys:
                return property in self.variable_to_property
            return property in (self.variable_to_property and filter_keys)
        return {
            self.variable_to_property.get(property): self._get_payload(
                self._get_variable_type(self.variable_types, property), value
            )
            for property, value in properties_data.items()
            if should_include(property)
        }


VARIABLE_PROPERTY_NAMES = {
    "car_id": "Car ID",
    "availability": "Availability",
    "maker": "Maker",
    "model": "Model",
    "submodel": "Submodel",
    "fuel_type": "Fuel Type",
    "mileage": "Mileage",
    "price": "Price",
    "insurance_inspection": "Insurance & Insection",
    "comment": "Comment",
    "from_year": "From Year",
    "year": "Year",
    "location": "Location",
    "transmission": "Transmission",
    "badge_detail": "Badge Detail",
}

VARIABLE_TYPES = {
    "title": {"car_id"},
    "rich_text": {"model", "submodel", "badge", "badge_detail", "comment", "location", "transmission"},
    "number": {"from_year", "year", "mileage", "price"},
    "select": {"availability", "maker", "insurance_inspection", "fuel_type"},
    "date": {"modified_date"},
}


def main():
    now = datetime.now()
    print(f"{now = }")
    print(f"{now.isoformat() = }")

    input_data = {
        "mileage": 80000,
    }

    pgenerator = PayloadGenerator(VARIABLE_PROPERTY_NAMES, VARIABLE_TYPES)
    generated = pgenerator.generate_properties_payload(input_data)
    print(f"{generated = }")


if __name__ == "__main__":
    main()
