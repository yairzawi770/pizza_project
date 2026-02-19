import os
import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import redis


MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
REDIS_URI     = os.getenv("REDIS_URI", "redis://localhost:6379")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]

redis_host, redis_port = REDIS_URI.replace("redis://", "").split(":")
redis_client = redis.Redis(host=redis_host, port=int(redis_port), decode_responses=True)


def main():
    consumer = KafkaConsumer(
        topics="pizza-orders",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="kitchen-team",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    print("🍳 Kitchen Worker מוכן – ממתין להזמנות...")

    for msg in consumer:
        order = msg.value
        order_id   = order["order_id"]
        pizza_type = order.get("pizza_type", "Unknown")

        print(f"📦 קיבלתי מטען: {order_id} ({pizza_type}) – מאבטח 15 שניות...")
        time.sleep(15)

        # עדכון סטטוס ל-DELIVERED (אם לא נשרף ע"י Enricher)
        result = orders_col.update_one(
            {"order_id": order_id, "status": "PREPARING"},
            {"$set": {"status": "DELIVERED"}}
        )

        if result.modified_count:
            print(f"✅ {order_id} עודכן ל-DELIVERED")
        else:
            print(f"⚠️  {order_id} לא עודכן (כנראה BURNT או כבר DELIVERED)")

        # Cache Invalidation
        deleted = redis_client.delete(f"order:{order_id}")
        if deleted:
            print(f"🗑️  Cache נמחק עבור {order_id}")


if __name__ == "__main__":
    main()
