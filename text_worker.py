"""
מבצע מגש פיצה 🍕 - Worker ב' – "מעבד הטקסט" (text_worker.py)

Part 1 spec:
  - מאזין ל-Topic: pizza-orders
  - group_id: 'text-team'
  - עבור כל הזמנה מבצע שלושה דברים:

  1. ALLERGY DETECTION – חיפוש מילות מפתח מזהירות:
       "allergy", "peanut", "gluten"
       אם נמצאה אחת → allergies_flagged = True

  2. TEXT SANITIZATION – ניקוי הטקסט:
       - הסרת סימני פיסוק
       - המרה לאותיות גדולות (UPPERCASE)
       - שמירה תחת השדה: cleaned_protocol

  3. MONGO UPDATE – עדכון שקט של שני השדות ב-MongoDB
"""

import os
import re
import json
import time
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TEXT_WORKER] %(message)s"
)
log = logging.getLogger("text_worker")

# ── ENV ──────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI",       "mongodb://localhost:27017")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ── MongoDB ──────────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]

# ── מילות מפתח מזהירות (Part 1, section Worker ב') ──────────
# כל מילה שמופיעה ב-special_instructions מדליקה דגל אדום
ALLERGY_KEYWORDS = ["allergy", "allergic", "peanut", "gluten", "dairy"]


# ── Helpers ──────────────────────────────────────────────────

def detect_allergies(text: str) -> bool:
    """
    מחפש מילות מפתח מזהירות בטקסט.
    החיפוש אינו תלוי-רישיות (case-insensitive).
    מחזיר True אם נמצאה לפחות מילה אחת.

    דוגמאות שיעבירו True:
      "I have a severe PEANUT allergy!!!"  → True  (peanut + allergy)
      "GLUTEN allergy!!!"                 → True  (gluten + allergy)
      "severe aLlErGy"                    → True  (allergy)
      "Make the crust extra crispy"       → False
    """
    text_lower = text.lower()
    for keyword in ALLERGY_KEYWORDS:
        if keyword in text_lower:
            log.info(f"  🚨 Keyword found: '{keyword}'")
            return True
    return False


def clean_text(text: str) -> str:
    """
    ניקוי טקסט (התממה):
      1. הסרת כל סימני פיסוק
      2. צמצום רצפי רווחים לרווח יחיד
      3. המרה ל-UPPERCASE

    דוגמאות:
      "I have a severe PEANUT allergy!!!"
        → "I HAVE A SEVERE PEANUT ALLERGY"

      "Radio silence\nMaintain cover\nAcknowledge\n!!!"
        → "RADIO SILENCE MAINTAIN COVER ACKNOWLEDGE"

      "   Check   the   perimeter   \t!   Awaiting signal.   "
        → "CHECK THE PERIMETER AWAITING SIGNAL"

      "'); DROP TABLE targets; -- just kidding"
        → "  DROP TABLE TARGETS  JUST KIDDING"
    """
    if not text:
        return ""

    # שלב 1: הסר כל תו שאינו אות, ספרה, או רווח
    sanitized = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)

    # שלב 2: החלף כל רצף whitespace (רווחים, tabs, newlines) ברווח יחיד
    sanitized = re.sub(r"\s+", " ", sanitized).strip()

    # שלב 3: UPPERCASE – יצירת "פרוטוקול מבצעי" נקי
    return sanitized.upper()


# ── Kafka Consumer with retry ────────────────────────────────

def _create_consumer(retries: int = 15, delay: int = 5) -> KafkaConsumer:
    """
    מנסה להתחבר ל-Kafka עד 'retries' פעמים.
    ממתין 'delay' שניות בין כל ניסיון.
    Kafka לוקח זמן לעלות לאחר docker-compose up.
    """
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
            log.info("✅ Text Worker connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            log.warning(
                f"⏳ Kafka not ready – attempt {attempt}/{retries}, "
                f"sleeping {delay}s …"
            )
            time.sleep(delay)

    raise RuntimeError("❌ Cannot connect to Kafka after all retries")


# ── Main processing loop ─────────────────────────────────────

def process_order(order: dict) -> None:
    """
    מעבד הזמנה בודדת:
      1. זיהוי אלרגיות
      2. ניקוי טקסט
      3. עדכון MongoDB
    """
    order_id             = order.get("order_id", "???")
    pizza_type           = order.get("pizza_type", "Unknown")
    special_instructions = order.get("special_instructions", "") or ""

    log.info(f"📋 Processing order {order_id} ({pizza_type})")
    log.info(f"   Raw instructions: {repr(special_instructions)}")

    # ── Step 1: Allergy / keyword detection ─────────────────
    allergies_flagged = detect_allergies(special_instructions)
    log.info(f"   allergies_flagged = {allergies_flagged}")

    # ── Step 2: Text sanitization ────────────────────────────
    cleaned_protocol = clean_text(special_instructions)
    log.info(f"   cleaned_protocol  = {repr(cleaned_protocol)}")

    # ── Step 3: MongoDB update ───────────────────────────────
    # מעדכן בשקט את שני השדות בלבד – לא נוגע בשדה status
    orders_col.update_one(
        {"order_id": order_id},
        {
            "$set": {
                "allergies_flagged": allergies_flagged,
                "cleaned_protocol":  cleaned_protocol,
            }
        },
    )
    log.info(f"   ✅ MongoDB updated for {order_id}")


def main():
    consumer = _create_consumer()
    log.info("📡 Text Worker ready – listening on topic 'pizza-orders' …")
    log.info(f"   Allergy keywords: {ALLERGY_KEYWORDS}")

    for message in consumer:
        order = message.value
        try:
            process_order(order)
        except Exception as e:
            # לא קורסים על הזמנה אחת פגומה – ממשיכים
            log.error(f"❌ Error processing order {order.get('order_id')}: {e}")


if __name__ == "__main__":
    main()
