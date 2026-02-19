import os
import re
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# טעינת ספר המתכונים
PREP_FILE = os.path.join(os.path.dirname(__file__), "pizza_prep.json")
with open(PREP_FILE, "r") as f:
    PIZZA_PREP: dict = json.load(f)

mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[^\w\s]", " ", text)   
    return text.upper()


def main():
    consumer = KafkaConsumer(
        "pizza-orders",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="text-team",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for msg in consumer:
        order = msg.value
        order_id   = order["order_id"]
        pizza_type = order.get("pizza_type", "")

        raw_instructions = order.get("special_instructions", "")
        clean_instructions = clean_text(raw_instructions)

        raw_prep = PIZZA_PREP.get(pizza_type, "")
        clean_prep = clean_text(raw_prep)

        orders_col.update_one(
            {"order_id": order_id},
            {"$set": {"cleaned_protocol": clean_instructions}}
        )

        payload = {
            "order_id":          order_id,
            "pizza_type":        pizza_type,
            "clean_instructions": clean_instructions,
            "clean_prep":        clean_prep,
        }
        producer.send("cleaned-instructions", value=payload)

    producer.flush()


if __name__ == "__main__":
    main()
