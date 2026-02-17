"""
מבצע מגש פיצה 🍕 - Enricher (Worker ב' - חלק ב')
"מומחה התוספות": מנתח רכיבים, קובע מטא-דאטה, מסמן BURNT אם לא כשר.
"""

import os
import json
import logging
import redis
from kafka import KafkaConsumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ENRICHER] %(message)s")

MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")
REDIS_URI     = os.getenv("REDIS_URI", "redis://localhost:6379")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ─── טעינת רשימות ─────────────────────────────────────────
LISTS_FILE = os.path.join(os.path.dirname(__file__), "pizza_analysis_lists.json")
with open(LISTS_FILE, "r", encoding="utf-8") as f:
    LISTS: dict = json.load(f)

ALLERGENS        = [w.upper() for w in LISTS["common_allergens"]]
FORBIDDEN_KOSHER = [w.upper() for w in LISTS["forbidden_non_kosher"]]
MEAT_INGREDIENTS = [w.upper() for w in LISTS["meat_ingredients"]]
DAIRY_INGREDIENTS= [w.upper() for w in LISTS["dairy_ingredients"]]

# ─── חיבורים ──────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]

redis_host, redis_port = REDIS_URI.replace("redis://", "").split(":")
redis_client = redis.Redis(host=redis_host, port=int(redis_port), decode_responses=True)

REDIS_TTL = 5  # שניות (להרגיש את ה-latency)


def find_hits(text: str, word_list: list) -> list:
    """מחפש כמה מילים מהרשימה מופיעות ב-text (substring)"""
    return [w for w in word_list if w in text]


def analyze_pizza_type(pizza_type: str, full_text: str) -> dict:
    """
    ניתוח הפיצה לפי רשימות הסגורות.
    מנסה לשלוף מ-Redis לפני ניתוח מחדש.
    """
    cache_key = f"pizza_meta:{pizza_type.upper().replace(' ','_')}"
    cached = redis_client.get(cache_key)

    if cached:
        logging.info(f"⚡ Cache HIT עבור {pizza_type}")
        return json.loads(cached)

    logging.info(f"🔍 Cache MISS – מנתח {pizza_type}")

    hits_allergens        = find_hits(full_text, ALLERGENS)
    hits_forbidden_kosher = find_hits(full_text, FORBIDDEN_KOSHER)
    hits_meat             = find_hits(full_text, MEAT_INGREDIENTS)
    hits_dairy            = find_hits(full_text, DAIRY_INGREDIENTS)

    # ─── לוגיקת קביעת שדות ──────────────────────────────
    # ברירת מחדל: כל פיצה חלבית ומכילה גלוטן
    is_meat   = len(hits_meat) > 0
    is_dairy  = True   # ברירת מחדל
    is_gluten = True   # ברירת מחדל

    # VEGAN → אין חלב
    if "VEGAN" in full_text:
        is_dairy = False
    elif hits_dairy:
        is_dairy = True

    # GLUTEN-FREE → אין גלוטן
    if "GLUTEN FREE" in full_text or "GLUTEN-FREE" in full_text:
        is_gluten = False

    # ─── כשרות ──────────────────────────────────────────
    has_forbidden = len(hits_forbidden_kosher) > 0
    has_meat_and_dairy = is_meat and is_dairy
    is_kosher = not (has_forbidden or has_meat_and_dairy)

    # ─── אלרגיות מה-instructions ─────────────────────────
    allergies_flagged = any(w in full_text for w in ["ALLERGY", "ALLERGIC", "PEANUT", "GLUTEN", "DAIRY"])

    meta = {
        "is_meat":            is_meat,
        "is_dairy":           is_dairy,
        "is_gluten":          is_gluten,
        "is_kosher":          is_kosher,
        "allergies_flagged":  allergies_flagged,
        "hits_allergens":     hits_allergens,
        "hits_forbidden":     hits_forbidden_kosher,
        "hits_meat":          hits_meat,
        "hits_dairy":         hits_dairy,
    }

    # שמירה ב-Redis עם TTL של 5 שניות
    redis_client.setex(cache_key, REDIS_TTL, json.dumps(meta))
    return meta


def main():
    consumer = KafkaConsumer(
        "cleaned-instructions",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="enricher-team",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    logging.info("🔬 Enricher מוכן – מחכה להוראות מנוקות...")

    for msg in consumer:
        payload = msg.value
        order_id          = payload["order_id"]
        pizza_type        = payload.get("pizza_type", "")
        clean_instructions= payload.get("clean_instructions", "")
        clean_prep        = payload.get("clean_prep", "")

        # טקסט משולב לניתוח
        full_text = f"{clean_instructions} {clean_prep}"

        meta = analyze_pizza_type(pizza_type, full_text)

        # קביעת סטטוס: BURNT אם לא כשר
        new_status = "BURNT" if not meta["is_kosher"] else None

        update_doc = {
            "is_meat":           meta["is_meat"],
            "is_dairy":          meta["is_dairy"],
            "is_gluten":         meta["is_gluten"],
            "is_kosher":         meta["is_kosher"],
            "allergies_flagged": meta["allergies_flagged"],
            "hits_summary": {
                "allergens": meta["hits_allergens"],
                "forbidden":  meta["hits_forbidden"],
                "meat":       meta["hits_meat"],
                "dairy":      meta["hits_dairy"],
            }
        }

        if new_status:
            update_doc["status"] = new_status
            logging.warning(f"🔥 {order_id} מסומן BURNT – לא כשר! (forbidden: {meta['hits_forbidden']})")
        else:
            logging.info(f"✅ {order_id} – כשר (meat={meta['is_meat']}, dairy={meta['is_dairy']})")

        orders_col.update_one(
            {"order_id": order_id},
            {"$set": update_doc}
        )


if __name__ == "__main__":
    main()
