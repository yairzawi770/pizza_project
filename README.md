# 🍕 מבצע "מגש פיצה" – תיעוד מלא בעברית

---

## 🎯 מה הפרויקט הזה בכלל?

"פיצריה" לכאורה – אבל בפועל **מערכת מידע מבצעית** של סוכנות ביון.  
כל "פיצה" היא מטען, כל "אלרגיה" היא קוד לחשד במכשיר מעקב.

המערכת בנויה כ-**Microservices** עם 4 שירותים עיקריים שמתקשרים דרך **Kafka** ושומרים נתונים ב-**MongoDB** ו-**Redis**.

---

## 🏗️ ארכיטקטורה כללית

```
לקוח/סוכן
    │
    ▼
FastAPI (api.py)           ← שער הכניסה
    │  שמירה ב-MongoDB (PREPARING)
    │  פרסום ל-Kafka: pizza-orders
    │
    ├──────────────────────────────────┐
    ▼                                  ▼
Kitchen Worker              Preprocessor (preprocessor.py)
(kitchen_worker.py)              │
    │                             │  ניקוי טקסט
    │  ממתין 15 שניות              │  פרסום ל-Kafka: cleaned-instructions
    │  → DELIVERED                │
    │  מוחק Redis Cache           ▼
    │                        Enricher (enricher.py)
    │                             │
    │                             │  ניתוח רכיבים
    │                             │  is_meat / is_dairy / is_kosher
    │                             │  → BURNT אם לא כשר
    │                             │
    └──────────── MongoDB ◄────────┘
                     │
                  Redis (Cache-Aside, TTL 60s / 5s)
```

---

## 📁 מבנה הקבצים

```
pizza_project/
├── docker-compose.yml        # תשתית מלאה
├── Dockerfile.api            # Docker לשרת ה-API
├── Dockerfile.worker         # Docker לכל ה-Workers
├── requirements.txt          # תלויות Python
│
├── api.py                    # FastAPI Gateway
├── kitchen_worker.py         # Worker א' – לוגיסטיקה
├── preprocessor.py           # Worker ב'1 – ניקוי טקסט
├── enricher.py               # Worker ב'2 – ניתוח וזיהוי
│
├── pizza_prep.json           # ספר המתכונים (~50 פיצות)
├── pizza_analysis_lists.json # רשימות: אלרגנים, כשרות, בשר, חלב
│
└── openshift/
    ├── stateful-services.yaml   # MongoDB, Redis, Kafka (StatefulSets)
    └── stateless-services.yaml  # API, Workers (Deployments)
```

---

## 🔄 זרימת נתונים מפורטת

### שלב 1: קליטת הזמנה
1. הלקוח שולח POST ל-`/orders/batch` עם קובץ JSON
2. ה-API שומר כל הזמנה ב-MongoDB עם `status: PREPARING`
3. ה-API מפרסם כל הזמנה כהודעה ל-Kafka topic: **pizza-orders**

### שלב 2: שני Workers עובדים **במקביל** על אותה הזמנה

#### 🍳 Kitchen Worker (group_id: kitchen-team)
- מאזין ל-`pizza-orders`
- **ממתין 15 שניות** (דימוי אבטחת מטען)
- מעדכן `status → DELIVERED` (רק אם הסטטוס עדיין PREPARING)
- **מוחק** את המפתח מ-Redis (Cache Invalidation)

#### 🧹 Preprocessor (group_id: text-team)
- מאזין ל-`pizza-orders`
- לוקח את `special_instructions` ומנקה: מוחק פיסוק, ממיר ל-UPPERCASE
- שולף את הוראות ההכנה מ-`pizza_prep.json` לפי `pizza_type`
- מנקה גם את הוראות ההכנה
- מפרסם ל-Kafka topic: **cleaned-instructions**

#### 🔬 Enricher (group_id: enricher-team)
- מאזין ל-`cleaned-instructions`
- **בודק Cache Redis** לפי `pizza_type` (TTL: 5 שניות)
  - **Cache Hit** → משתמש בנתונים הקיימים (מהיר!)
  - **Cache Miss** → מנתח מחדש ושומר ב-Redis
- מבצע **substring matching** מול 4 רשימות:
  - `common_allergens` → `allergies_flagged`
  - `forbidden_non_kosher` → בדיקת כשרות
  - `meat_ingredients` → `is_meat`
  - `dairy_ingredients` → `is_dairy`
- קובע: `is_kosher`, `is_gluten`
- **אם לא כשר → `status: BURNT`**
- מעדכן MongoDB

### שלב 3: שליפת סטטוס
`GET /order/{order_id}` מיישם **Cache-Aside**:
1. בדיקה ב-Redis (מפתח: `order:{order_id}`)
2. Hit → מחזיר עם `"source": "redis_cache"`
3. Miss → שולף מ-MongoDB, שומר ב-Redis (60 שניות), מחזיר עם `"source": "mongodb"`

---

## 📊 לוגיקת הניתוח (Enricher)

### ברירות מחדל
| שדה | ברירת מחדל |
|------|------------|
| `is_dairy` | `true` (כל פיצה חלבית כברירת מחדל) |
| `is_gluten` | `true` (כל פיצה מכילה גלוטן כברירת מחדל) |
| `is_meat` | `false` (נקבע לפי רשימה) |

### חריגים
| מילה בטקסט | השפעה |
|------------|--------|
| `VEGAN` | `is_dairy = false` |
| `GLUTEN FREE` | `is_gluten = false` |

### כשרות
פיצה **אינה כשרה** (`is_kosher = false`) אם:
- יש רכיב מ-`forbidden_non_kosher` (חזיר, שרימפס, שפמנון...)
- **או** יש גם רכיב בשרי **וגם** רכיב חלבי (בשר בחלב!)

**פיצה לא כשרה → `status: BURNT` 🔥**

---

## 🚀 הפעלה מקומית

```bash
# שלב 1: העתק את קבצי הנתונים
cp pizza_prep.json pizza_analysis_lists.json pizza_project/

# שלב 2: הפעל את הכל
cd pizza_project
docker compose up --build

# שלב 3: שלח הזמנות
curl -X POST "http://localhost:8000/orders/batch" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@pizza_orders.json"

# שלב 4: בדוק סטטוס (ב-15 שניות הראשונות → PREPARING)
curl http://localhost:8000/order/order_1002

# שלב 5: אחרי 20 שניות → DELIVERED (או BURNT אם לא כשר)
curl http://localhost:8000/order/order_1002
```

---

## ☁️ פריסה ל-OpenShift

```bash
# שירותי נתונים (StatefulSets עם PVC)
oc apply -f openshift/stateful-services.yaml

# שירותים אפליקטיביים (Deployments)
oc apply -f openshift/stateless-services.yaml

# בדיקה
oc get pods
oc get routes
```

### עקרונות הפריסה
| שירות | סוג | סיבה |
|--------|------|-------|
| MongoDB, Redis, Kafka | **StatefulSet** | צריכים זהות רשת יציבה ו-PVC |
| API, Preprocessor, Enricher, Kitchen | **Deployment** | Stateless – ניתן לסקייל |

---

## 🧪 דוגמאות ניתוח

### order_1002 – Hawaiian, "I have a severe PEANUT allergy!!!"
- `allergies_flagged: true` (מילת קוד: PEANUT)
- Hawaiian = ham + cheese → **בשר + חלב** → `is_kosher: false`
- **→ status: BURNT** 🔥

### order_1016 – Vegan Delight, "Dairy free, gluten free, allergic to peanut oil!"
- `is_dairy: false` (VEGAN)
- `is_gluten: false` (GLUTEN FREE)
- `allergies_flagged: true`
- ללא רכיב אסור → `is_kosher: true`
- **→ status: DELIVERED** ✅

---

## 💡 נקודות חשובות

1. **שני Workers מאזינים לאותו Topic** אבל ב-**group_id שונה** – כל אחד מקבל את כל ההודעות.
2. **Kitchen Worker** מעדכן DELIVERED רק אם הסטטוס עדיין PREPARING (כדי לא לדרוס BURNT).
3. **Redis TTL של 5 שניות** ל-pizza metadata – כדי לחוש את ההבדל בין Cache Hit ל-Miss.
4. **Redis TTL של 60 שניות** ל-order status – נמחק ע"י Kitchen Worker לאחר DELIVERED.
5. **Substring matching** – לא exact match, מספיק שהמילה מופיעה בתוך הטקסט.
