import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

now = datetime.now()

def random_past_datetime(days=30):
    return now - timedelta(
        days=random.randint(0, days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

def generate_row_hash():
    return uuid.uuid4().hex

def generate_work_orders(n=100):
    equipment_ids = []

    for i in range(50):
        equipment_ids.append(f"E_{i+1}")

    data = []

    for _ in range(n):
        opened = random_past_datetime(90)
        closed = opened + timedelta(days=random.randint(1,10)) if random.random() > 0.3 else None

        data.append({
            "work_order_id": f"WID_{_+1}",
            "equipment_id": random.choice(equipment_ids),
            "work_order_type": random.choice(["PREVENTIVE", "CORRECTIVE", "EMERGENCY"]),
            "opened_date": opened.date(),
            "closed_date": closed.date() if closed else None,
            "technician_id": f"TECH_{random.randint(1,10)}",
            "parts_cost_usd": round(random.uniform(100, 5000), 2),
            "labor_hours": round(random.uniform(1, 20), 2),
            "labor_cost_usd": round(random.uniform(50, 2000), 2),
            "description": "maintenance task",
            "_ingested_at": now,
            "_row_hash": generate_row_hash()
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    maintenance_work_df = generate_work_orders()

    script_path = Path(__file__).resolve()
    output_path = f"{script_path.parents[1]}/output/work_orders.csv"
    maintenance_work_df.to_csv(output_path, index=False)

    print("Data generated")