import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

now = datetime.now()

def generate_site():
    data = []

    for i in range(1, 6):
        data.append({
            "site_id": f"SITE_{i}",
            "site_name": f"Landfill_{i}",
            "state": random.choice(["TN", "KA", "MH"]),
            "permit_capacity_tons_daily": random.randint(500, 2000),
            "current_airspace_remaining_tons": random.randint(10000, 50000),
            "_ingested_at": now
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    site_df = generate_site()

    script_path = Path(__file__).resolve()
    output_path = f"{script_path.parents[1]}/output/site_details.csv"
    site_df.to_csv(output_path, index=False)

    print("Data generated")