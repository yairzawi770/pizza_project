"""
מבצע מגש פיצה 🍕 - Enricher – "מומחה התוספות" (enricher.py)

Part 2 spec:
  - מאזין ל-cleaned-instructions, group_id='enricher-team'
  - עבור כל הודעה:
      1. בדיקת Redis לפי pizza_type (TTL: 5 שניות)
         Cache Hit  → שימוש בנתונים קיימים
         Cache Miss → ניתוח מלא + שמירה ב-Redis
      2. ניתוח טקסטואלי (substring) מול 4 רשימות:
           common_allergens, forbidden_non_kosher,
           meat_ingredients, dairy_ingredients
      3. קביעת שדות:
           allergies_flagged  – keyword מ-special_instructions ("allergy","peanut","gluten")
           is_meat            – מתוך meat_ingredients
           is_dairy           – ברירת מחדל TRUE (אלא אם VEGAN)
           is_gluten          – ברירת מחדל TRUE (אלא אם GLUTEN-FREE)
           is_kosher          – FALSE אם: forbidden OR (meat AND dairy)
      4. אם not is_kosher → status = BURNT 🔥
      5. עדכון MongoDB

כלל הכשרות (Part 2, section 6.4):
  is_kosher = False  אם:
    - יש רכיב מ-forbidden_non_kosher
    - OR נמצא שילוב בשר + חלב

ברירות מחדל (Part 2, section 6.3):
  - כל פיצה: is_dairy=True, is_gluten=True
  - VEGAN   → is_dairy=False
  - GLUTEN-FREE → is_gluten=False
"""

import os, json, time, logging
from datetime import datetime
import redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ENRICHER] %(message)s")
log = logging.getLogger("enricher")

MONGO_URI     = os.getenv("MONGO_URI",      "mongodb://localhost:27017")
REDIS_URI     = os.getenv("REDIS_URI",      "redis://localhost:6379")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
REDIS_META_TTL = 5   # שניות (Part 2, section 7.2)

# ── Load closed lists ───────────────────────────────────────
_LISTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pizza_analysis_lists.json")
with open(_LISTS_PATH, "r", encoding="utf-8") as _f:
    _LISTS = json.load(_f)

ALLERGENS         = [w.upper() for w in _LISTS["common_allergens"]]
FORBIDDEN_KOSHER  = [w.upper() for w in _LISTS["forbidden_non_kosher"]]
MEAT_INGREDIENTS  = [w.upper() for w in _LISTS["meat_ingredients"]]
DAIRY_INGREDIENTS = [w.upper() for w in _LISTS["dairy_ingredients"]]

# ── Connections ─────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
orders_col   = mongo_client["pizza_ops"]["orders"]

_h, _p = REDIS_URI.replace("redis://", "").split(":")
redis_client = redis.Redis(host=_h, port=int(_p), decode_responses=True)


# ── Analysis helpers ─────────────────────────────────────────
def _hits(text: str, word_list: list) -> list:
    """Substring matching – כל מילה מהרשימה שמופיעה ב-text."""
    return [w for w in word_list if w in text]


def _analyze_pizza_type(pizza_type: str, full_text: str) -> dict:
    """
    ניתוח מטא-דאטה של סוג פיצה.
    Part 2, section 7.2:
      Key   = pizza_type
      Value = רשימת Hits
      TTL   = 5 שניות
    """
    cache_key = f"pizza_type:{pizza_type.upper().replace(' ', '_')}"

    # Cache Hit
    cached = redis_client.get(cache_key)
    if cached:
        log.info(f"⚡ Cache HIT – {pizza_type}")
        return json.loads(cached)

    # Cache Miss – full analysis
    log.info(f"🔍 Cache MISS – analyzing {pizza_type}")

    hits_forbidden = _hits(full_text, FORBIDDEN_KOSHER)
    hits_meat      = _hits(full_text, MEAT_INGREDIENTS)
    hits_dairy     = _hits(full_text, DAIRY_INGREDIENTS)
    hits_allergens = _hits(full_text, ALLERGENS)

    # ── Defaults (section 6.3) ──────────────────────────────
    is_dairy  = True    # ברירת מחדל: כל פיצה חלבית
    is_gluten = True    # ברירת מחדל: כל פיצה מכילה גלוטן

    if "VEGAN" in full_text:
        is_dairy = False           # VEGAN → אין חלב

    if "GLUTEN FREE" in full_text or "GLUTEN-FREE" in full_text:
        is_gluten = False          # GLUTEN-FREE → אין גלוטן

    is_meat = len(hits_meat) > 0

    # אם נמצאו רכיבי חלב בטקסט – הפיצה חלבית (גם אם לא VEGAN)
    if hits_dairy:
        is_dairy = True

    # ── Kosher rule (section 6.4) ───────────────────────────
    has_forbidden      = len(hits_forbidden) > 0
    has_meat_and_dairy = is_meat and is_dairy
    is_kosher          = not (has_forbidden or has_meat_and_dairy)

    meta = {
        "is_meat":         is_meat,
        "is_dairy":        is_dairy,
        "is_gluten":       is_gluten,
        "is_kosher":       is_kosher,
        "hits_allergens":  hits_allergens,
        "hits_forbidden":  hits_forbidden,
        "hits_meat":       hits_meat,
        "hits_dairy":      hits_dairy,
    }

    # שמירה ב-Redis – TTL 5 שניות
    redis_client.setex(cache_key, REDIS_META_TTL, json.dumps(meta))
    return meta


def _create_consumer(retries: int = 15, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(
                "cleaned-instructions",
                bootstrap_servers=KAFKA_SERVERS,
                group_id="enricher-team",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            log.info("✅ Enricher connected to Kafka")
            return c
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka not ready – attempt {attempt}/{retries}, sleeping {delay}s …")
            time.sleep(delay)
    raise RuntimeError("Cannot connect to Kafka")


def main():
    consumer = _create_consumer()
    log.info("🔬 Enricher ready – waiting for cleaned instructions …")

    for msg in consumer:
        p          = msg.value
        order_id   = p.get("order_id", "???")
        pizza_type = p.get("pizza_type", "")

        clean_instructions = p.get("clean_instructions", "")
        clean_prep         = p.get("clean_prep", "")

        # טקסט משולב לניתוח
        full_text = f"{clean_instructions} {clean_prep}"

        # ── allergies_flagged (Part 1 text_worker logic, carried into Enricher) ──
        # מחפש מילות מפתח מזהירות: "allergy", "peanut", "gluten"
        ALLERGY_KEYWORDS = ["ALLERGY", "ALLERGIC", "PEANUT", "GLUTEN", "DAIRY"]
        allergies_flagged = any(kw in clean_instructions for kw in ALLERGY_KEYWORDS)

        # ── Pizza-type metadata (with Redis cache) ──────────────
        meta = _analyze_pizza_type(pizza_type, full_text)

        # ── Build MongoDB update ────────────────────────────────
        update_fields = {
            "allergies_flagged": allergies_flagged,   # Part 1: suspected_tracker equivalent
            "is_meat":           meta["is_meat"],
            "is_dairy":          meta["is_dairy"],
            "is_gluten":         meta["is_gluten"],
            "is_kosher":         meta["is_kosher"],
            "hits_summary": {
                "allergens": meta["hits_allergens"],
                "forbidden": meta["hits_forbidden"],
                "meat":      meta["hits_meat"],
                "dairy":     meta["hits_dairy"],
            },
            "insertion_time": datetime.utcnow().isoformat(),  # Part 3: timestamp
        }

        # ── Kosher / BURNT rule (section 6.4) ───────────────────
        if not meta["is_kosher"]:
            update_fields["status"] = "BURNT"
            log.warning(
                f"🔥 {order_id} → BURNT "
                f"(forbidden={meta['hits_forbidden']}, "
                f"meat={meta['is_meat']}, dairy={meta['is_dairy']})"
            )
        else:
            log.info(
                f"✅ {order_id} is kosher "
                f"(meat={meta['is_meat']}, dairy={meta['is_dairy']}, "
                f"gluten={meta['is_gluten']})"
            )

        orders_col.update_one(
            {"order_id": order_id},
            {"$set": update_fields},
        )
        log.info(f"📝 MongoDB updated for {order_id}")


if __name__ == "__main__":
    main()
