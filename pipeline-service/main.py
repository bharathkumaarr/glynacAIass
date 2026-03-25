from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import engine, get_db, Base
from models.customer import Customer
from services.ingestion import run_ingestion

Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/ingest")
def ingest():
    try:
        count = run_ingestion()
        return {"status": "success", "records_processed": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/customers")
def get_customers(page: int = 1, limit: int = 10, db: Session = Depends(get_db)):
    offset = (page - 1) * limit
    total = db.query(Customer).count()
    items = db.query(Customer).offset(offset).limit(limit).all()

    return {
        "data": [serialize(c) for c in items],
        "total": total,
        "page": page,
        "limit": limit
    }


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    c = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="customer not found")
    return serialize(c)


def serialize(c):
    return {
        "customer_id": c.customer_id,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "email": c.email,
        "phone": c.phone,
        "address": c.address,
        "date_of_birth": str(c.date_of_birth) if c.date_of_birth else None,
        "account_balance": float(c.account_balance) if c.account_balance else None,
        "created_at": c.created_at.isoformat() if c.created_at else None
    }
