"""
מבצע מגש פיצה 🍕 - API Gateway (FastAPI)
שער הכניסה למערכת: מקבל הזמנות, שומר ב-MongoDB, ומפרסם ל-Kafka
"""

import os
import json
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import redis
from kafka import KafkaProducer

# ─── הגדרות סביבה ─────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
REDIS_URI     = os.getenv("REDIS_URI", "redis://localhost:6379")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ─── חיבורים ──────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
db           = mongo_client["pizza_ops"]
orders_col   = db["orders"]

redis_host, redis_port = REDIS_URI.replace("redis://", "").split(":")
redis_client = redis.Redis(host=redis_host, port=int(redis_port), decode_responses=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ─── מודל Pydantic ────────────────────────────────────────
class PizzaOrder(BaseModel):
    order_id:             str
    pizza_type:           str
    size:                 str
    quantity:             int
    is_delivery:          bool
    special_instructions: Optional[str] = ""


app = FastAPI(title="🍕 Pizza Ops API", version="2.0")


# ─── POST /orders/batch ───────────────────────────────────
@app.post("/orders/batch", summary="העלאת קובץ הזמנות JSON")
async def upload_orders(file: UploadFile = File(...)):
    """
    קולט קובץ JSON עם מערך הזמנות.
    שומר כל הזמנה ב-MongoDB (סטטוס PREPARING).
    מפרסם כל הזמנה ל-Kafka topic: pizza-orders.
    """
    contents = await file.read()
    try:
        orders: List[dict] = json.loads(contents)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="קובץ JSON לא תקין")

    inserted = []
    for raw in orders:
        order = PizzaOrder(**raw)
        doc = order.dict()
        doc["status"] = "PREPARING"

        # שמירה ב-MongoDB (upsert למניעת כפילויות)
        orders_col.update_one(
            {"order_id": doc["order_id"]},
            {"$set": doc},
            upsert=True
        )

        # פרסום ל-Kafka
        producer.send("pizza-orders", value=doc)
        inserted.append(doc["order_id"])

    producer.flush()
    return {"message": f"נקלטו {len(inserted)} הזמנות", "order_ids": inserted}


# ─── POST /orders (single order) ─────────────────────────
@app.post("/orders", summary="הוספת הזמנה בודדת")
async def create_order(order: PizzaOrder):
    doc = order.dict()
    doc["status"] = "PREPARING"

    orders_col.update_one(
        {"order_id": doc["order_id"]},
        {"$set": doc},
        upsert=True
    )
    producer.send("pizza-orders", value=doc)
    producer.flush()
    return {"message": "הזמנה נקלטה", "order_id": doc["order_id"]}


# ─── GET /order/{order_id} ────────────────────────────────
@app.get("/order/{order_id}", summary="שליפת סטטוס הזמנה")
async def get_order(order_id: str):
    """
    Cache-Aside:
    1. בדוק ב-Redis
    2. אם חסר – שלוף מ-MongoDB ושמור ב-Redis (60 שניות)
    """
    cached = redis_client.get(f"order:{order_id}")
    if cached:
        data = json.loads(cached)
        data["source"] = "redis_cache"
        return data

    doc = orders_col.find_one({"order_id": order_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="הזמנה לא נמצאה")

    redis_client.setex(f"order:{order_id}", 60, json.dumps(doc))
    doc["source"] = "mongodb"
    return doc


# ─── GET /health ──────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "service": "api"}
