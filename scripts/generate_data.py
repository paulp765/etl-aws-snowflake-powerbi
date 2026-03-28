import pandas as pd
import json
import random
from datetime import datetime, timedelta
import os

# ── Reproducible randomness ─────────────────────────
random.seed(42)

# ── Helper: random date in 2024 ─────────────────────
def random_date():
    start = datetime(2024, 1, 1)
    return (start + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")

# ════════════════════════════════════════════════════
#  1. SALES TRANSACTIONS  →  CSV
# ════════════════════════════════════════════════════
products = [
    ("P001", "Laptop",     75000),
    ("P002", "Mouse",       850),
    ("P003", "Keyboard",   2500),
    ("P004", "Monitor",   18000),
    ("P005", "Headphones",  4500),
    ("P006", "Webcam",     3200),
    ("P007", "USB Hub",    1200),
    ("P008", "SSD Drive",  6500),
]

regions    = ["North", "South", "East", "West"]
categories = ["Electronics", "Accessories", "Peripherals"]

rows = []
for i in range(1, 501):                          # 500 transactions
    pid, pname, price = random.choice(products)
    qty      = random.randint(1, 10)
    discount = round(random.uniform(0, 0.25), 2) # 0–25 % discount
    revenue  = round(price * qty * (1 - discount), 2)

    rows.append({
        "transaction_id": f"TXN{i:04d}",
        "customer_id":    f"CUST{random.randint(1, 100):03d}",
        "product_id":     pid,
        "product_name":   pname,
        "category":       random.choice(categories),
        "region":         random.choice(regions),
        "quantity":       qty,
        "unit_price":     price,
        "discount":       discount,
        "total_revenue":  revenue,
        "transaction_date": random_date(),
        "status":         random.choice(["Completed", "Completed", "Completed", "Returned", "Pending"]),
    })

sales_df = pd.DataFrame(rows)

# Introduce a few NULLs to make cleaning realistic
null_indices = random.sample(range(500), 15)
for idx in null_indices:
    sales_df.loc[idx, random.choice(["discount", "region"])] = None

os.makedirs("data/raw", exist_ok=True)
sales_df.to_csv("data/raw/sales_transactions.csv", index=False)
print(f"✅ sales_transactions.csv — {len(sales_df)} rows")


# ════════════════════════════════════════════════════
#  2. CUSTOMER PROFILES  →  JSON
# ════════════════════════════════════════════════════
cities = [
    ("Mumbai", "Maharashtra"),  ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"), ("Bangalore", "Karnataka"),
    ("Chennai", "Tamil Nadu"),  ("Hyderabad", "Telangana"),
    ("Pune", "Maharashtra"),    ("Ahmedabad", "Gujarat"),
]
segments   = ["Premium", "Regular", "Occasional"]
genders    = ["Male", "Female", "Other"]

customers = []
for i in range(1, 101):                          # 100 customers
    city, state = random.choice(cities)
    join_date   = (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")

    customers.append({
        "customer_id":    f"CUST{i:03d}",
        "first_name":     f"Customer_{i}",
        "last_name":      f"Lastname_{i}",
        "email":          f"customer{i}@example.com",
        "phone":          f"+91-{random.randint(7000000000, 9999999999)}",
        "city":           city,
        "state":          state,
        "age":            random.randint(22, 60),
        "gender":         random.choice(genders),
        "segment":        random.choice(segments),
        "join_date":      join_date,
        "loyalty_points": random.randint(0, 5000),
        "is_active":      random.choice([True, True, True, False]),
    })

with open("data/raw/customer_profiles.json", "w") as f:
    json.dump(customers, f, indent=2)
print(f"✅ customer_profiles.json — {len(customers)} customers")


# ════════════════════════════════════════════════════
#  3. QUICK VALIDATION
# ════════════════════════════════════════════════════
print("\n── Sales Data Preview ──────────────────────")
print(sales_df.head(3).to_string(index=False))
print(f"\nNull values:\n{sales_df.isnull().sum()[sales_df.isnull().sum() > 0]}")
print(f"Total revenue: ₹{sales_df['total_revenue'].sum():,.0f}")

print("\n── Customer Data Preview ───────────────────")
cust_df = pd.DataFrame(customers)
print(cust_df[["customer_id","first_name","city","segment","is_active"]].head(3).to_string(index=False))
print(f"Active customers: {cust_df['is_active'].sum()}")