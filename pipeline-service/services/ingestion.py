import os
import requests
import dlt
from sqlalchemy.dialects.postgresql import insert
from database import SessionLocal
from models.customer import Customer
from datetime import datetime, date
from decimal import Decimal

MOCK_SERVER_URL = os.getenv("MOCK_SERVER_URL", "http://mock-server:5000")


def fetch_all_customers():
    all_data = []
    page = 1
    limit = 10

    while True:
        res = requests.get(f"{MOCK_SERVER_URL}/api/customers", params={"page": page, "limit": limit})
        res.raise_for_status()
        body = res.json()
        all_data.extend(body["data"])

        if page * limit >= body["total"]:
            break
        page += 1

    return all_data


def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_datetime(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except ValueError:
        return None


def upsert_customers(data):
    db = SessionLocal()
    count = 0

    try:
        for item in data:
            stmt = insert(Customer).values(
                customer_id=item["customer_id"],
                first_name=item["first_name"],
                last_name=item["last_name"],
                email=item["email"],
                phone=item["phone"],
                address=item["address"],
                date_of_birth=parse_date(item.get("date_of_birth")),
                account_balance=Decimal(str(item.get("account_balance", 0))),
                created_at=parse_datetime(item.get("created_at"))
            ).on_conflict_do_update(
                index_elements=["customer_id"],
                set_={
                    "first_name": item["first_name"],
                    "last_name": item["last_name"],
                    "email": item["email"],
                    "phone": item["phone"],
                    "address": item["address"],
                    "date_of_birth": parse_date(item.get("date_of_birth")),
                    "account_balance": Decimal(str(item.get("account_balance", 0))),
                    "created_at": parse_datetime(item.get("created_at"))
                }
            )
            db.execute(stmt)
            count += 1

        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

    return count


def run_ingestion():
    data = fetch_all_customers()
    count = upsert_customers(data)
    return count
