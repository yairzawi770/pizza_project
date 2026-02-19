# 🍕 מבצע "מגש פיצה" – חלק 3

---

## מה חדש בחלק 3?

חלק 3 מוסיף **שכבת ניתוח סיכונים ודשבורד ניהולי** למערכת.

---

## 🆕 רכיבים חדשים

### 1️⃣ **עדכון ל-Enricher** ✅
- **שדה חדש**: `insertion_time` (ISO 8601 timestamp)
- **מתי**: בסיום עיבוד ההזמנה, לפני עדכון MongoDB
- **מטרה**: מדידת זמני עיבוד, הערכת שיפורים מ-caching

---

### 2️⃣ **Risk Evaluator** 🚨 (קצין סיכונים)

**קובץ**: `risk_evaluator.py`

#### תפקיד
בודק כל הזמנה מול רשימת `common_allergens` וקובע האם לבטל אותה.

#### חוק ביטול
```python
if substring_match(cleaned_protocol, common_allergens):
    status = "CANCELLED"
```

**דוגמה**:
```
Order: order_1002
cleaned_protocol: "I HAVE A SEVERE PEANUT ALLERGY"
allergens: ["PEANUT"]

→ "PEANUT" in text → CANCELLED ✅
```

#### Case Normalization (חובה!)
- הטקסט הנקי כבר ב-UPPERCASE (מהPreprocessor)
- רשימת האלרגנים גם מומרת ל-UPPERCASE
- חיפוש substring על שני הצדדים נורמליזציה → לא מפספס בגלל רישיות

#### מה הוא מעדכן ב-MongoDB
```python
{
    "status": "CANCELLED",                    # אם נמצאה התאמה
    "allergens_matched": ["PEANUT", "DAIRY"], # רשימת ההתאמות
    "update_time": "2026-02-19T14:35:22.123Z" # תמיד מעודכן
}
```

#### Pandas Analysis
- טוען הזמנות ל-`pd.DataFrame`
- ספירה לפי סטטוס: `df['status'].value_counts()`
- Top 10 allergens: `pd.Series(allergens).value_counts().head(10)`

#### Logging ל-stdout
```
2026-02-19 14:35:20 [RISK_EVALUATOR] ============================================
2026-02-19 14:35:20 [RISK_EVALUATOR] 🔍 Starting risk scan...
2026-02-19 14:35:20 [RISK_EVALUATOR]    Loaded 20 orders from MongoDB
2026-02-19 14:35:20 [RISK_EVALUATOR] 🚨 CANCELLED: order_1002 | allergens: PEANUT, DAIRY
2026-02-19 14:35:20 [RISK_EVALUATOR] ✅ Scan complete:
2026-02-19 14:35:20 [RISK_EVALUATOR]    Total orders scanned: 20
2026-02-19 14:35:20 [RISK_EVALUATOR]    Cancelled this scan:  1
2026-02-19 14:35:20 [RISK_EVALUATOR]    Top allergens causing cancellations:
2026-02-19 14:35:20 [RISK_EVALUATOR]       PEANUT: 3
2026-02-19 14:35:20 [RISK_EVALUATOR]       DAIRY: 2
```

#### הגדרת Interval
```bash
SCAN_INTERVAL=10  # שניות בין כל סריקה (default: 10)
```

---

### 3️⃣ **Streamlit Dashboard** 📊

**קובץ**: `streamlit_dashboard.py`  
**פורט**: `http://localhost:8501`

#### תצוגות (חובה):

**א. Pie Chart – התפלגת סטטוסים**
```
PREPARING:  45%
DELIVERED:  40%
CANCELLED:  10%
BURNT:       5%

Total Orders: 200
```

**ב. Bar Chart – Top 10 Allergens**
```
PEANUT     ████████████ 12
DAIRY      █████████ 9
GLUTEN     ██████ 6
SHELLFISH  ████ 4
```
*רק מהזמנות CANCELLED*

**ג. טבלה – 10 הזמנות אחרונות**
| order_id | pizza_type | status | allergens_matched | update_time |
|----------|------------|--------|-------------------|-------------|
| order_2050 | Hawaiian | CANCELLED | [PEANUT] | 2026-02-19T14:35:22Z |
| order_2049 | Veggie | DELIVERED | [] | 2026-02-19T14:35:18Z |
| ... | ... | ... | ... | ... |

*ממוין לפי `update_time` יורד (החדשות למעלה)*

#### Cache
- `@st.cache_data(ttl=5)` – רענון אוטומטי כל 5 שניות
- לחצן "🔄 Refresh Data" – רענון ידני

---

## 🏗️ ארכיטקטורה מעודכנת

```
                    ┌─────────────────────┐
                    │   Streamlit         │
                    │   Dashboard         │
                    │   (Port 8501)       │
                    └─────────┬───────────┘
                              │ קורא
                              ▼
┌────────────────────────────────────────────────┐
│              MongoDB (pizza_ops.orders)         │
│                                                 │
│  Fields:                                        │
│  - order_id, pizza_type, status                │
│  - cleaned_protocol                            │
│  - insertion_time  ← Enricher                  │
│  - update_time     ← Risk Evaluator            │
│  - allergens_matched ← Risk Evaluator          │
└────────┬───────────────────────────────────────┘
         │                          ▲
         │ טוען                      │ מעדכן
         ▼                          │
┌─────────────────────────────────────────────────┐
│         Risk Evaluator (risk_evaluator.py)      │
│                                                  │
│  1. טוען הזמנות → DataFrame                     │
│  2. בודק substring matching                     │
│  3. מעדכן CANCELLED + allergens_matched         │
│  4. Pandas aggregations                         │
│  5. Logging ל-stdout                            │
│                                                  │
│  Loop: כל SCAN_INTERVAL שניות                   │
└──────────────────────────────────────────────────┘
```

---

## 🚀 הרצה

```bash
# בנייה והפעלה
docker compose up --build

# צפייה ב-logs של Risk Evaluator
docker compose logs -f risk_evaluator

# גישה לדשבורד
http://localhost:8501
```

---

## 📊 מבנה קבצים מלא

```
pizza_project/
├── api.py                      # Part 1
├── kitchen_worker.py           # Part 1
├── text_worker.py              # Part 1
├── preprocessor.py             # Part 2
├── enricher.py                 # Part 2 (+ insertion_time חדש)
├── risk_evaluator.py           # Part 3 🆕
├── streamlit_dashboard.py      # Part 3 🆕
│
├── docker-compose.yml          # עודכן: +2 services
├── Dockerfile.api
├── Dockerfile.workers
├── Dockerfile.streamlit        # 🆕
├── requirements.txt            # עודכן: pandas, streamlit, plotly
│
├── pizza_prep.json
├── pizza_analysis_lists.json
├── pizza_orders.json
└── openshift/
    ├── data-services.yaml
    └── app-services.yaml
```

---

## ✅ קריטריונים להצלחה

- [ ] `docker compose up --build` מצליח ללא קריסות
- [ ] Enricher מוסיף `insertion_time` לכל הזמנה
- [ ] Risk Evaluator:
  - [ ] מבצע substring matching עם case normalization
  - [ ] מעדכן `update_time` לכל הזמנה
  - [ ] מסמן CANCELLED עם `allergens_matched`
  - [ ] Logging מפורט ל-stdout
- [ ] Streamlit Dashboard זמין ב-`http://localhost:8501`:
  - [ ] Pie chart סטטוסים + Total Orders
  - [ ] Bar chart Top 10 allergens
  - [ ] טבלת 10 הזמנות אחרונות

---

## 🎯 דוגמת תרחיש מלא

```bash
1. API מקבל pizza_orders.json → MongoDB (PREPARING)
2. Kafka מפרסם → pizza-orders topic
3. Preprocessor מנקה טקסט → cleaned-instructions topic
4. Enricher מנתח + הוסף insertion_time → MongoDB
5. Risk Evaluator סורק:
   - order_1002 מכיל "PEANUT ALLERGY" → CANCELLED
   - מעדכן update_time, allergens_matched
   - לוג: "🚨 CANCELLED: order_1002 | allergens: PEANUT, DAIRY"
6. Streamlit מציג:
   - Pie: CANCELLED 5%
   - Bar: PEANUT (3), DAIRY (2)
   - Table: order_1002 בשורה ראשונה
```

---

🍕 **המערכת שלמה!**
