"""ShopStream seed data generator.

Generates the three historical (batch) datasets for the ShopStream lakehouse
project. Plain Python standard library on purpose: learners run this locally
with `python generate_seed_data.py` and get the same shape of data the guide
shows, no pip installs needed.

Outputs (into ./data):
  customers.csv          1,000 customers
  products.csv           200 products across 8 categories
  orders_2026_h1.csv     ~14,000 order lines, Jan 1 - Jun 30 2026
                         (intentionally dirty: duplicate order lines, a few
                         negative quantities, mixed-case statuses, some null
                         coupon codes)
"""

import csv
import os
import random

random.seed(42)  # same data for every learner

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

FIRST_NAMES = [
    "Aarav", "Priya", "Rahul", "Sneha", "Vikram", "Ananya", "Karan", "Divya",
    "Arjun", "Meera", "Rohan", "Isha", "Aditya", "Pooja", "Nikhil", "Riya",
    "James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia",
    "Lucas", "Mia", "Mason", "Amelia", "Diego", "Lucia", "Wei", "Yuki",
]
LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Kumar", "Gupta", "Reddy", "Nair", "Mehta",
    "Smith", "Johnson", "Brown", "Garcia", "Miller", "Davis", "Chen", "Tanaka",
    "Wilson", "Anderson", "Martinez", "Lopez",
]
CITIES = [
    ("Mumbai", "IN"), ("Bengaluru", "IN"), ("Delhi", "IN"), ("Hyderabad", "IN"),
    ("Pune", "IN"), ("Chennai", "IN"), ("New York", "US"), ("San Francisco", "US"),
    ("Austin", "US"), ("London", "GB"), ("Berlin", "DE"), ("Singapore", "SG"),
    ("Toronto", "CA"), ("Sydney", "AU"), ("Dubai", "AE"),
]
SIGNUP_CHANNELS = ["organic", "paid_search", "social", "referral", "email"]

CATEGORIES = {
    "electronics": ["Wireless Earbuds", "Mechanical Keyboard", "USB-C Hub", "Webcam", "Portable SSD", "Smartwatch", "Bluetooth Speaker"],
    "home-kitchen": ["Air Fryer", "Coffee Grinder", "Knife Set", "Blender", "Rice Cooker", "Toaster"],
    "fitness": ["Yoga Mat", "Dumbbell Set", "Resistance Bands", "Foam Roller", "Jump Rope"],
    "books": ["Data Engineering Handbook", "Python Crash Course", "Atomic Habits", "Deep Work", "The Pragmatic Programmer"],
    "fashion": ["Running Shoes", "Denim Jacket", "Cotton T-Shirt", "Baseball Cap", "Hoodie"],
    "beauty": ["Face Serum", "Sunscreen SPF50", "Lip Balm", "Shampoo Bar"],
    "toys": ["Building Blocks", "RC Car", "Puzzle 1000pc", "Board Game"],
    "grocery": ["Green Tea 100pk", "Almond Butter", "Dark Chocolate", "Trail Mix"],
}
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "returned", "cancelled"]
COUPONS = [None, None, None, None, None, "SAVE10", "WELCOME15", "FESTIVE20"]


def gen_customers(n=1000):
    rows = []
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, country = random.choice(CITIES)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        rows.append({
            "customer_id": f"C{i:05d}",
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "city": city,
            "country": country,
            "signup_date": f"2025-{month:02d}-{day:02d}",
            "signup_channel": random.choice(SIGNUP_CHANNELS),
        })
    return rows


def gen_products():
    rows = []
    pid = 1
    for category, names in CATEGORIES.items():
        for name in names:
            for variant in range(random.randint(3, 7)):
                base = round(random.uniform(4, 20) ** 2, 2)  # skew toward cheaper items
                rows.append({
                    "product_id": f"P{pid:04d}",
                    "product_name": f"{name} v{variant + 1}" if variant else name,
                    "category": category,
                    "unit_price": base,
                    "unit_cost": round(base * random.uniform(0.45, 0.75), 2),
                })
                pid += 1
    return rows[:200]


def gen_orders(customers, products, n_orders=8000):
    rows = []
    line_id = 1
    for i in range(1, n_orders + 1):
        order_id = f"O{i:06d}"
        customer = random.choice(customers)
        month = random.randint(1, 6)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        ts = f"2026-{month:02d}-{day:02d} {hour:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
        status = random.choice(ORDER_STATUSES)
        # ~8% of the time the upstream system logs status in caps — kept dirty on purpose
        if random.random() < 0.08:
            status = status.upper()
        n_lines = random.choices([1, 2, 3, 4], weights=[55, 27, 12, 6])[0]
        for _ in range(n_lines):
            product = random.choice(products)
            qty = random.choices([1, 2, 3, -1], weights=[70, 20, 8, 2])[0]  # -1 = bad rows to clean
            row = {
                "order_line_id": f"L{line_id:07d}",
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "quantity": qty,
                "unit_price": product["unit_price"],
                "order_ts": ts,
                "status": status,
                "coupon_code": random.choice(COUPONS) or "",
            }
            rows.append(row)
            line_id += 1
            # ~1.5% duplicated lines (simulates the source system double-firing)
            if random.random() < 0.015:
                rows.append(dict(row))
    return rows


def write_csv(name, rows):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


if __name__ == "__main__":
    customers = gen_customers()
    products = gen_products()
    orders = gen_orders(customers, products)
    write_csv("customers.csv", customers)
    write_csv("products.csv", products)
    write_csv("orders_2026_h1.csv", orders)
