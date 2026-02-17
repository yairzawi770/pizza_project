"""
מבצע מגש פיצה 🍕 - API Gateway
Part 1 requirements:
  POST /uploadfile     – קובץ JSON עם מערך הזמנות
  POST /orders/batch   – JSON body עם מערך (alias שמופיע ב-curl acceptance test)
  POST /orders         – הזמנה בודדת עם UUID חדש
  GET  /order/{id}     – Cache-Aside (Redis → MongoDB)
"""

import os, json, time, logging
from typing import List, Optional
from uuid import UUID, uuid4
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from pydantic import BaseModel, Field
from pymongo import MongoClient
import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
log = logging.getLogger("api")

# ── ENV ────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI",      "mongodb://localhost:27017")
REDIS_URI     = os.getenv("REDIS_URI",      "redis://localhost:6379")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")


# ── Pydantic model (Part 1 spec) ────────────────────────────
class PizzaOrder(BaseModel):
    order_id:             str = Field(default_factory=lambda: str(uuid4()))
    pizza_type:           str
    size:                 str
    quantity:             int
    is_delivery:          bool
    special_instructions: Optional[str] = Field(default="")


# ── Connection helpers ──────────────────────────────────────
def _kafka_producer(retries: int = 15, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            log.info("✅ Kafka producer connected")
            return p
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka not ready – attempt {attempt}/{retries}, waiting {delay}s …")
            time.sleep(delay)
    raise RuntimeError("Cannot connect to Kafka after retries")


mongo_client  = MongoClient(MONGO_URI)
orders_col    = mongo_client["pizza_ops"]["orders"]

_r_host, _r_port = REDIS_URI.replace("redis://", "").split(":")
redis_client  = redis.Redis(host=_r_host, port=int(_r_port), decode_responses=True)

producer = _kafka_producer()

app = FastAPI(title="🍕 Pizza Ops – Intelligence Gateway", version="2.0")


# ── Internal helpers ────────────────────────────────────────
def _save_and_publish(order_dict: dict) -> None:
    """שמור ב-MongoDB (PREPARING) + פרסם ל-Kafka."""
    doc = {**order_dict, "status": "PREPARING"}
    orders_col.update_one(
        {"order_id": doc["order_id"]},
        {"$set": doc},
        upsert=True,
    )
    producer.send("pizza-orders", value=doc)


# ── Endpoints ───────────────────────────────────────────────

@app.post("/uploadfile", summary="העלאת קובץ הזמנות (multipart)")
async def upload_file(file: UploadFile = File(...)):
    """
    POST /uploadfile
    קולט קובץ JSON עם מערך הזמנות.
    שומר כל הזמנה ב-MongoDB (PREPARING) ומפרסם ל-Kafka (בנפרד לכל הזמנה).
    """
    raw = await file.read()
    try:
        orders: list = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON file")

    ids = []
    for item in orders:
        o = PizzaOrder(**item)
        _save_and_publish(o.dict())
        ids.append(o.order_id)

    producer.flush()
    log.info(f"📥 /uploadfile – {len(ids)} orders ingested")
    return {"ingested": len(ids), "order_ids": ids}


@app.post("/orders/batch", summary="העלאת מערך הזמנות (JSON body)")
async def orders_batch(request: Request):
    """
    POST /orders/batch
    קולט JSON body (מערך הזמנות) – זהו ה-endpoint שמופיע ב-acceptance-test curl.
    """
    try:
        orders: list = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    ids = []
    for item in orders:
        o = PizzaOrder(**item)
        _save_and_publish(o.dict())
        ids.append(o.order_id)

    producer.flush()
    log.info(f"📥 /orders/batch – {len(ids)} orders ingested")
    return {"ingested": len(ids), "order_ids": ids}


@app.post("/orders", summary="הזמנה בודדת חדשה")
async def create_order(pizza_type: str, special_instructions: str = ""):
    """
    POST /orders
    פרמטרים: pizza_type (חובה), special_instructions (ברירת מחדל ריקה).
    יוצר UUID חדש, שומר ב-MongoDB, שולח ל-Kafka.
    """
    # UUID נוצר אוטומטית ע"י default_factory במודל
    order = PizzaOrder(
        pizza_type=pizza_type,
        size="Medium",
        quantity=1,
        is_delivery=False,
        special_instructions=special_instructions,
    )
    _save_and_publish(order.dict())
    producer.flush()
    log.info(f"📥 /orders – new order {order.order_id} ({pizza_type})")
    return {"order_id": order.order_id, "status": "PREPARING"}


@app.get("/order/{order_id}", summary="סטטוס הזמנה (Cache-Aside)")
async def get_order(order_id: str):
    """
    GET /order/{order_id}
    Cache-Aside:
      1. בדוק ב-Redis (key = order:{order_id})
         Hit  → מחזיר עם "source": "redis_cache"
      2. Miss → שולף מ-MongoDB, שומר ב-Redis (60 שניות)
         → מחזיר עם "source": "mongodb"
    """
    cached = redis_client.get(f"order:{order_id}")
    if cached:
        data = json.loads(cached)
        data["source"] = "redis_cache"
        return data

    doc = orders_col.find_one({"order_id": order_id}, {"_id": 0})
    if not doc:
        raise HTTPException(404, f"Order '{order_id}' not found")

    redis_client.setex(f"order:{order_id}", 60, json.dumps(doc))
    doc["source"] = "mongodb"
    return doc


@app.get("/health")
async def health():
    return {"status": "ok", "service": "api"}
