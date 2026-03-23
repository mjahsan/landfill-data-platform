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

def generate_transactions(n=500):
    data = []

    for _ in range(n):
        gross = random.uniform(5, 20)
        tare = random.uniform(2, 5)
        net = gross - tare
        rate = random.uniform(10, 50)

        data.append({
            "transaction_id": str(uuid.uuid4()),
            "site_id": f"SITE_{random.randint(1,5)}",
            "transaction_datetime": random_past_datetime(),
            "vehicle_id": f"VEH_{random.randint(1,50)}",
            "customer_id": f"CUST_{random.randint(1,100)}",
            "waste_type": random.choice(["MSW", "C&D", "INDUSTRIAL"]),
            "gross_weight_tons": round(gross, 2),
            "tare_weight_tons": round(tare, 2),
            "net_weight_tons": round(net, 2),
            "rate_per_ton_usd": round(rate, 2),
            "total_charge_usd": round(net * rate, 2),
            "_ingested_at": now
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    transactions_df = generate_transactions()

    script_path = Path(__file__).resolve()
    output_path = f"{script_path.parents[1]}/output/transactions.csv"
    transactions_df.to_csv(output_path, index=False)

    print("Data generated")