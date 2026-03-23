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

def generate_equipment_status_log(n=200):
    equipment_ids = []
    data = []

    for i in range(50):
        equipment_ids.append(f"E_{i+1}")

    statuses = ["OPERATIONAL", "UNDER_MAINTENANCE", "OUT_OF_SERVICE", "IDLE"]
    
    for _ in range(n):
        data.append({
            "status_event_id": f"EV_{_+1}",
            "equipment_id": random.choice(equipment_ids),
            "status": random.choice(statuses),
            "recorded_at": random_past_datetime(),
            "operator_id": f"OP_{random.randint(1,10)}",
            "notes": "auto-generated",
            "_ingested_at": now
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    equipment_status_log_df = generate_equipment_status_log()

    script_path = Path(__file__).resolve()
    output_path = f"{script_path.parents[1]}/output/equipment_status_log.csv"
    equipment_status_log_df.to_csv(output_path, index=False)

    print("Data generated")