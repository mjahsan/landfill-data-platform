import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

now = datetime.now()

def generate_row_hash():
    return uuid.uuid4().hex

def generate_equipment(n=50):
    data = []
    for i in range(n):
        equipment_id = f"E_{i+1}"

        data.append({
            "equipment_id": equipment_id,
            "equipment_name": f"Machine_{i+1}",
            "equipment_type": random.choice(["COMPACTOR", "BULLDOZER", "GRADER"]),
            "manufacturer": random.choice(["CAT", "Komatsu", "Volvo"]),
            "model_number": f"Model_{random.randint(100,999)}",
            "purchase_date": datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000)),
            "site_id": f"SITE_{random.randint(1,5)}",
            "_source_file": random.choice(["crm_update", "purchase_order", "direct_purchase"]),
            "_ingested_at": now,
            "_row_hash": generate_row_hash()
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    equipment_df = generate_equipment()

    script_path = Path(__file__).resolve()
    output_path = f"{script_path.parents[1]}/output/equipment.csv"
    equipment_df.to_csv(output_path, index=False)

    print("Data generated")