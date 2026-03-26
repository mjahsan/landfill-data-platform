import psycopg2
import yaml
from pathlib import Path
from datetime import datetime, date, timedelta, time
import random

#----------------------------------------------------------------------------
# Extracting path for yaml configuration 
current_dir = Path(__file__).resolve()
parent_dir = current_dir.parents[2]
environ_var_dir = f"{parent_dir}/environ_var/environ_var.yml"

# Extracting variables
with open(environ_var_dir) as f:
    config = yaml.safe_load(f)
database = config['project_database']
user = config["project_user"]
password = config['password']

#----------------------------------------------------------------------------

# Establishing connection for PostgreSQL
DB_CONN = {
    "host": "localhost",
    "port": 5432,
    "database": database,
    "user": user,
    "password": password
}

conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()

#----------------------------------------------------------------------------

# Creating tables
print ("Creating tables...")
cur.execute('''
            DROP TABLE IF EXISTS customers;
            CREATE TABLE IF NOT EXISTS customers (
            customer_id    SERIAL PRIMARY KEY,
            customer_name  VARCHAR(120) NOT NULL,
            customer_type  VARCHAR(50),
            contact_number VARCHAR(20),
            email          VARCHAR(100),
            address        TEXT,
            registered_on  DATE
        );

        DROP TABLE IF EXISTS vehicles;
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id         SERIAL PRIMARY KEY,
            vehicle_number     VARCHAR(30) NOT NULL UNIQUE,
            vehicle_type       VARCHAR(60),
            owner_customer_id  INT REFERENCES customers(customer_id),
            capacity_tons      NUMERIC(8,2)
        );
 
        DROP TABLE IF EXISTS landfill_master;
        CREATE TABLE IF NOT EXISTS landfill_master (
            landfill_id            SERIAL PRIMARY KEY,
            site_name              VARCHAR(120) NOT NULL,
            location               VARCHAR(150),
            region                 VARCHAR(80),
            operating_since        DATE,
            licensed_capacity_tons NUMERIC(12,2),
            operator_name          VARCHAR(100),
            compliance_status      VARCHAR(30) DEFAULT 'Compliant'
        );

        DROP TABLE IF EXISTS transactions;
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id       SERIAL PRIMARY KEY,
            landfill_id          INT REFERENCES landfill_master(landfill_id),
            customer_id          INT REFERENCES customers(customer_id),
            vehicle_id           INT REFERENCES vehicles(vehicle_id),
            transaction_date     DATE NOT NULL,
            transaction_time     TIME NOT NULL,
            waste_category       VARCHAR(80),
            incoming_weight_tons NUMERIC(8,3),
            charge_per_ton       NUMERIC(8,2),
            total_charge         NUMERIC(12,2),
            payment_status       VARCHAR(20)
        );
 
        DROP TABLE IF EXISTS landfill_operational_status;
        CREATE TABLE IF NOT EXISTS landfill_operational_status (
            status_id               SERIAL PRIMARY KEY,
            landfill_id             INT REFERENCES landfill_master(landfill_id) UNIQUE,
            last_updated_date       DATE NOT NULL,
            total_capacity_tons     NUMERIC(12,2),
            used_capacity_tons      NUMERIC(12,2),
            remaining_capacity_tons NUMERIC(12,2),
            utilization_pct         NUMERIC(5,2)
        );
 
        DROP TABLE IF EXISTS landfill_alerts;
        CREATE TABLE IF NOT EXISTS landfill_alerts (
            alert_id                  SERIAL PRIMARY KEY,
            landfill_id               INT REFERENCES landfill_master(landfill_id),
            alert_date                DATE NOT NULL,
            utilization_pct_at_alert  NUMERIC(5,2),
            threshold_pct             NUMERIC(5,2) DEFAULT 80.00,
            alert_level               VARCHAR(20),
            alert_message             TEXT,
            is_acknowledged           BOOLEAN DEFAULT FALSE
        );

 ''')

conn.commit()
print("Tables Created\n")

#----------------------------------------------------------------------------

# Customer details for insertion
print ("Inserting customers...")
customers = [
    ("Chennai Municipal Corp",   "Municipal",   "9876543210", "cmc@chennai.gov",       "Ripon Buildings, Chennai"),
    ("GreenBuild Constructions", "Commercial",  "9123456780", "info@greenbuild.in",     "Anna Nagar, Chennai"),
    ("Tamil Nadu Industries",    "Industrial",  "9988776655", "contact@tni.in",         "Ambattur Industrial Estate"),
    ("Swachh Waste Services",    "Commercial",  "9871234560", "swachh@waste.in",        "Tambaram, Chennai"),
    ("City Cleaners Pvt Ltd",    "Municipal",   "9001122334", "city@cleaners.in",       "T Nagar, Chennai"),
    ("Raj Transport Co",         "Individual",  "8765432109", "raj@transport.in",       "Sholinganallur, Chennai"),
    ("Metro Infra Ltd",          "Industrial",  "9444555666", "metro@infra.in",         "Perungudi, Chennai"),
]

# Inserting customer details
print ("Inserting customers...")
for name, wtype, phone, email, address in customers:
    customer_ids = []
    cur.execute('''
        INSERT INTO customers (customer_name, customer_type, contact_number, email, address, registered_on)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING customer_id
    ''', (name, wtype, phone, email, address, date(random.randint(2010, 2020), random.randint(1,12), 1))
    )
    customer_ids.append(cur.fetchone()[0])
conn.commit()
print(f"Customers added: {len(customer_ids)}")

#----------------------------------------------------------------------------

# Vehicle details for insertion
print ("Inserting vehicles...")
vehicles = [
    ("TN01AB1234", "Compactor Truck",  0, 8.0), 
    ("TN02CD5678", "Tipper Truck",     1, 10.0),
    ("TN03EF9012", "Mini Loader",      2, 4.0),
    ("TN04GH3456", "Bulk Carrier",     3, 12.0),
    ("TN05IJ7890", "Compactor Truck",  4, 8.0),
    ("TN06KL1234", "Tractor Trailer",  5, 15.0),
    ("TN07MN5678", "Tipper Truck",     6, 10.0),
    ("TN08OP9012", "Mini Loader",      0, 4.0),
    ("TN09QR3456", "Bulk Carrier",     1, 12.0),
    ("TN10ST7890", "Compactor Truck",  2, 8.0),
]

for vnum, vtype, customer, wtons in vehicles:
    vehicle_ids = []
    cur.execute('''
        INSERT INTO vehicles (vehicle_number, vehicle_type, owner_customer_id, capacity_tons)
        VALUES (%s, %s, %s, %s)
        RETURNING vehicle_id
    ''', (vnum, vtype, customer, wtons)
    )
    vehicle_ids.append(cur.fetchone()[0])
conn.commit()
print(f"Vehicles added: {len(vehicle_ids)}")

#----------------------------------------------------------------------------

# Landfill details for insertion
print("Inserting landfill details...")
landfills = [
    ("Chennai North Landfill",          "Manadi, Chennai",           "North Chennai", date(2008, 4, 1),  150000.00),
    ("Tambaram Waste Processing Site",  "Tambaram, Chennai",         "South Chennai", date(2010, 7, 15), 120000.00),
    ("Sholinganallur Disposal Facility","Sholinganallur, Chennai",   "East Chennai",  date(2012, 1, 10), 90000.00),
    ("Ambattur Municipal Landfill",     "Ambattur, Chennai",         "West Chennai",  date(2005, 9, 20), 200000.00),
    ("Perungudi Integrated Waste Site", "Perungudi, Chennai",        "Central Chennai",date(2015,3, 5),  80000.00),
]

for site_name, location, region, op_since, capacity in landfills:
    landfill_ids = []
    cur.execute('''
        INSERT INTO landfill_master (site_name, location, region, operating_since, licensed_capacity_tons, operator_name, compliance_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING landfill_id
    ''', (site_name, location, region, op_since, capacity)
    )
    landfill_ids.append(cur.fetchone()[0])
conn.commit()
print(f"Landfills added: {len(landfill_ids)}")
    
#----------------------------------------------------------------------------

# Daily Transactions for landfill (365 days)
waste_categories = [
    "Municipal Solid Waste",
    "Construction & Demolition",
    "Industrial Dry Waste",
    "Organic / Bio-degradable",
    "Inert Waste",
]
charge_rates = {
    "Municipal Solid Waste":       (250, 400),
    "Construction & Demolition":   (300, 500),
    "Industrial Dry Waste":        (350, 600),
    "Organic / Bio-degradable":    (200, 350),
    "Inert Waste":                 (150, 300),
}

payment_statuses = ["Paid", "Paid", "Paid", "Pending", "Waived"]


end_date = date.now()
start_date = end_date - timedelta(364)
all_dates = [start_date + timedelta(i) for i in range(365)]

print ("Inserting daily transactions...")

total_transaction = 0

for single_date in all_dates:
    for landfill in landfill_ids:
        num_transactions = random.randint (50,100)
        for _ in range(num_transactions):
            customer_id = random.choice(customer_ids)
            vehicle_id = random.choice(vehicle_ids)
            category = random.choice(waste_categories)
            weight = round(random.uniform(1.5, 12), 3)
            lo, hi = charge_rates[category]
            charge_per_t = round(random.uniform(lo, hi), 2)
            total = round(weight * charge_per_t, 2)
            payment = random.choice(payment_statuses)
            rand_time = time((random.randint(8, 17)), (random.randint(0, 59)), (random.randint(0, 59)))

            cur.execute ("""
                INSERT INTO transactions
                    (landfill_id, customer_id, vehicle_id, transaction_date, transaction_time,
                    waste_category, incoming_weight_tons, charge_per_ton, total_charge, payment_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (landfill, customer_id, vehicle_id, single_date, rand_time,
                      category, weight, charge_per_t, total, payment)
            )

            total_transaction += 1
    conn.commit()
print(f"Transactions added: {total_transaction}")

