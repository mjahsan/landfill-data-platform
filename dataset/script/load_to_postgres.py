from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# Connection string
engine = create_engine(
    "postgresql://landfill_user:password123@localhost:5432/landfill"
)

# Retrieving path
original_path = Path(__file__).resolve()
base_path = f"{original_path.parents[1]}/output"

tables = {
    "equipment": "equipment.csv",
    "equipment_status_log": "equipment_status_log.csv",
    "maintenance_work_orders": "work_orders.csv",
    "landfill_transactions": "transactions.csv",
    "site_dim": "site_details.csv"
}

for table, file in tables.items():
    try:
        path = f"{base_path}/{file}"

        print(f"Loading {table}...")

        df = pd.read_csv(path)

        df.to_sql(
            table,
            engine,
            if_exists="replace",   # ONLY for first run
            index=False
        )

        print(f"Loaded {table}")
    
    except Exception as e:
        print (f"Failed loading {table}: {e}")

print("All tables loaded successfully")