"""
מבצע מגש פיצה 🍕 - Risk Evaluator (Part 3)
"קצין סיכונים" - בודק התאמות אלרגנים ומבטל הזמנות

חוק ביטול:
  אם יש substring match בין clean_special_instructions לבין
  אחד מה-common_allergens → CANCELLED
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd
from pymongo import MongoClient

# ── Logging setup ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RISK_EVALUATOR] %(message)s"
)
log = logging.getLogger("risk_evaluator")

# ── ENV ──────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "10"))  # seconds

# ── MongoDB ──────────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
orders_col = mongo_client["pizza_ops"]["orders"]

# ── Load allergens list ──────────────────────────────────────
LISTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "pizza_analysis_lists.json"
)
with open(LISTS_PATH, "r", encoding="utf-8") as f:
    LISTS = json.load(f)

COMMON_ALLERGENS = [a.upper() for a in LISTS["common_allergens"]]

log.info(f"📋 Loaded {len(COMMON_ALLERGENS)} allergens: {COMMON_ALLERGENS}")


# ── Core logic ───────────────────────────────────────────────

def check_allergen_match(clean_text: str, allergens: List[str]) -> List[str]:
    """
    בודק substring matching (case-insensitive).
    
    Args:
        clean_text: טקסט נקי (כבר UPPERCASE)
        allergens: רשימת אלרגנים (כבר UPPERCASE)
    
    Returns:
        רשימת אלרגנים שנמצאו
    """
    matched = []
    text_upper = clean_text.upper()  # normalize לוודא
    
    for allergen in allergens:
        if allergen in text_upper:
            matched.append(allergen)
    
    return matched


def load_orders_to_dataframe() -> pd.DataFrame:
    """
    טוען הזמנות מ-MongoDB ל-DataFrame.
    מחזיר רק הזמנות שעברו enrichment (יש להן cleaned_protocol).
    """
    cursor = orders_col.find(
        {"cleaned_protocol": {"$exists": True}},
        {
            "_id": 0,
            "order_id": 1,
            "pizza_type": 1,
            "status": 1,
            "cleaned_protocol": 1,
            "insertion_time": 1,
            "update_time": 1,
            "allergens_matched": 1,
        }
    )
    
    df = pd.DataFrame(list(cursor))
    
    if df.empty:
        log.warning("⚠️  No orders found with cleaned_protocol")
        return df
    
    # ברירות מחדל למקרה שהשדות לא קיימים
    if "allergens_matched" not in df.columns:
        df["allergens_matched"] = None
    if "update_time" not in df.columns:
        df["update_time"] = None
    
    return df


def scan_and_cancel() -> Dict:
    """
    סריקה מלאה:
      1. טוען הזמנות ל-DataFrame
      2. בודק כל הזמנה מול רשימת אלרגנים
      3. מעדכן MongoDB
      4. מחזיר סטטיסטיקות
    """
    log.info("=" * 60)
    log.info("🔍 Starting risk scan...")
    
    df = load_orders_to_dataframe()
    
    if df.empty:
        log.info("   No orders to scan")
        return {"total": 0, "cancelled": 0, "top_allergens": {}}
    
    log.info(f"   Loaded {len(df)} orders from MongoDB")
    
    cancelled_count = 0
    all_matched_allergens = []
    
    for idx, row in df.iterrows():
        order_id = row["order_id"]
        clean_text = row.get("cleaned_protocol", "")
        current_status = row.get("status", "")
        
        # בדיקת התאמה
        matched = check_allergen_match(clean_text, COMMON_ALLERGENS)
        
        update_doc = {
            "update_time": datetime.utcnow().isoformat()
        }
        
        if matched:
            # ביטול! CANCELLED מנצח כל סטטוס אחר
            update_doc["status"] = "CANCELLED"
            update_doc["allergens_matched"] = matched
            
            orders_col.update_one(
                {"order_id": order_id},
                {"$set": update_doc}
            )
            
            cancelled_count += 1
            all_matched_allergens.extend(matched)
            
            log.warning(
                f"🚨 CANCELLED: {order_id} | "
                f"allergens: {', '.join(matched)}"
            )
        else:
            # לא נמצאה התאמה - רק עדכון timestamp
            orders_col.update_one(
                {"order_id": order_id},
                {"$set": update_doc}
            )
    
    # ── Pandas aggregation: Top allergens ────────────────────
    if all_matched_allergens:
        allergen_series = pd.Series(all_matched_allergens)
        top_allergens = allergen_series.value_counts().head(10).to_dict()
    else:
        top_allergens = {}
    
    log.info("─" * 60)
    log.info(f"✅ Scan complete:")
    log.info(f"   Total orders scanned: {len(df)}")
    log.info(f"   Cancelled this scan:  {cancelled_count}")
    
    if top_allergens:
        log.info(f"   Top allergens causing cancellations:")
        for allergen, count in list(top_allergens.items())[:5]:
            log.info(f"      {allergen}: {count}")
    
    log.info("=" * 60)
    
    return {
        "total": len(df),
        "cancelled": cancelled_count,
        "top_allergens": top_allergens,
    }


def main():
    log.info("🚀 Risk Evaluator started")
    log.info(f"   Scan interval: {SCAN_INTERVAL} seconds")
    log.info(f"   Allergens monitored: {len(COMMON_ALLERGENS)}")
    
    while True:
        try:
            stats = scan_and_cancel()
        except Exception as e:
            log.error(f"❌ Error during scan: {e}", exc_info=True)
        
        log.info(f"💤 Sleeping for {SCAN_INTERVAL} seconds...\n")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
