import os
import re
import json
import time
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

# ── ENV ──────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI",       "mongodb://localhost:27017")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ── MongoDB ──────────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]

ALLERGY_KEYWORDS = ["allergy", "peanut", "gluten"]


def detect_allergies(text: str) -> bool:
    text_lower = text.lower()
    for keyword in ALLERGY_KEYWORDS:
        if keyword in text_lower:
            print(f"  🚨 Keyword found: '{keyword}'")
            return True
    return False


def clean_text(text: str) -> str:
    if not text:
        return ""

    sanitized = re.sub(r"[^\w\s]", " ", text)

    return sanitized.upper()


# ── Kafka Consumer with retry ────────────────────────────────

def _create_consumer(retries: int = 15, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                "pizza-orders",                     # Topic לקריאה
                bootstrap_servers=KAFKA_SERVERS,
                group_id="text-team",               # Part 1 spec
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",       # קרא מההתחלה אם אין offset
                enable_auto_commit=True,
            )
            print("✅ Text Worker connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            print(
                f"⏳ Kafka not ready – attempt {attempt}/{retries}, "
                f"sleeping {delay}s …"
            )
            time.sleep(delay)

    raise RuntimeError("❌ Cannot connect to Kafka after all retries")


# ── Main processing loop ─────────────────────────────────────

def process_order(order: dict) -> None:
    order_id             = order.get("order_id", "???")
    special_instructions = order.get("special_instructions", "")

    allergies_flagged = detect_allergies(special_instructions)
    print(f"   allergies_flagged = {allergies_flagged}")

    cleaned_protocol = clean_text(special_instructions)
    print(f"   cleaned_protocol  = {cleaned_protocol}")

    orders_col.update_one(
        {"order_id": order_id},
        {
            "$set": {
                "allergies_flagged": allergies_flagged,
                "cleaned_protocol":  cleaned_protocol,
            }
        },
    )
    print(f"   ✅ MongoDB updated for {order_id}")


def main():
    consumer = _create_consumer()
    print("📡 Text Worker ready – listening on topic 'pizza-orders' …")
    print(f"   Allergy keywords: {ALLERGY_KEYWORDS}")

    for message in consumer:
        order = message.value
        try:
            process_order(order)
        except Exception as e:
            print(f"❌ Error processing order {order.get('order_id')}: {e}")


if __name__ == "__main__":
    main()
