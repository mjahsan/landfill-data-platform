"""
Tables populated:
  1. customers
  2. vehicles
  3. landfill_master
  4. transactions
  5. landfill_operational_status
  6. landfill_alerts

Usage:
  pip install psycopg2-binary
  python landfill_data_generator.py
"""

import psycopg2
import random
from datetime import date, timedelta, time

# ─── CONFIG — update these ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "DBNAME",
    "user":     "YOUR USER",
    "password": "YOUR PASSWORD",
}
# ─────────────────────────────────────────────────────────────────────────────


# ─── STATIC SEED DATA ─────────────────────────────────────────────────────────

CUSTOMERS = [
    ("Chennai Municipal Corp",   "Municipal",   "9876543210", "cmc@chennai.gov",       "Ripon Buildings, Chennai"),
    ("GreenBuild Constructions", "Commercial",  "9123456780", "info@greenbuild.in",     "Anna Nagar, Chennai"),
    ("Tamil Nadu Industries",    "Industrial",  "9988776655", "contact@tni.in",         "Ambattur Industrial Estate"),
    ("Swachh Waste Services",    "Commercial",  "9871234560", "swachh@waste.in",        "Tambaram, Chennai"),
    ("City Cleaners Pvt Ltd",    "Municipal",   "9001122334", "city@cleaners.in",       "T Nagar, Chennai"),
    ("Raj Transport Co",         "Individual",  "8765432109", "raj@transport.in",       "Sholinganallur, Chennai"),
    ("Metro Infra Ltd",          "Industrial",  "9444555666", "metro@infra.in",         "Perungudi, Chennai"),
]

VEHICLES = [
    ("TN01AB1234", "Compactor Truck",  0, 8.0),   # owner index, capacity
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

LANDFILLS = [
    ("Chennai North Landfill",          "Manali, Chennai",           "North Chennai", date(2008, 4, 1),  150000.00),
    ("Tambaram Waste Processing Site",  "Tambaram, Chennai",         "South Chennai", date(2010, 7, 15), 120000.00),
    ("Sholinganallur Disposal Facility","Sholinganallur, Chennai",   "East Chennai",  date(2012, 1, 10), 90000.00),
    ("Ambattur Municipal Landfill",     "Ambattur, Chennai",         "West Chennai",  date(2005, 9, 20), 200000.00),
    ("Perungudi Integrated Waste Site", "Perungudi, Chennai",        "Central Chennai",date(2015,3, 5),  80000.00),
]

WASTE_CATEGORIES = [
    "Municipal Solid Waste",
    "Construction & Demolition",
    "Industrial Dry Waste",
    "Organic / Bio-degradable",
    "Inert Waste",
]

# Charge range (₹ per ton) per waste category
CHARGE_RATES = {
    "Municipal Solid Waste":       (250, 400),
    "Construction & Demolition":   (300, 500),
    "Industrial Dry Waste":        (350, 600),
    "Organic / Bio-degradable":    (200, 350),
    "Inert Waste":                 (150, 300),
}

PAYMENT_STATUSES = ["Paid", "Paid", "Paid", "Pending", "Waived"]

ALERT_THRESHOLD = 80.0   # trigger alert at 80% capacity


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def random_time_of_day():
    """Returns a random time between 06:00 and 20:00."""
    hour   = random.randint(6, 20)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return time(hour, minute, second)


def get_alert_level(utilization_pct):
    if utilization_pct >= 100:
        return "Full",     "Landfill has reached 100% capacity. Immediate action required."
    elif utilization_pct >= 90:
        return "Critical", f"Landfill is at {utilization_pct:.1f}% capacity. Critical threshold breached."
    else:
        return "Warning",  f"Landfill has crossed {ALERT_THRESHOLD}% capacity threshold."


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # ── 1. CREATE TABLES ──────────────────────────────────────────────────────
    print("Creating tables...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id    SERIAL PRIMARY KEY,
            customer_name  VARCHAR(120) NOT NULL,
            customer_type  VARCHAR(50),
            contact_number VARCHAR(20),
            email          VARCHAR(100),
            address        TEXT,
            registered_on  DATE
        );

        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id         SERIAL PRIMARY KEY,
            vehicle_number     VARCHAR(30) NOT NULL UNIQUE,
            vehicle_type       VARCHAR(60),
            owner_customer_id  INT REFERENCES customers(customer_id),
            capacity_tons      NUMERIC(8,2)
        );

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

        CREATE TABLE IF NOT EXISTS landfill_operational_status (
            status_id               SERIAL PRIMARY KEY,
            landfill_id             INT REFERENCES landfill_master(landfill_id) UNIQUE,
            last_updated_date       DATE NOT NULL,
            total_capacity_tons     NUMERIC(12,2),
            used_capacity_tons      NUMERIC(12,2),
            remaining_capacity_tons NUMERIC(12,2),
            utilization_pct         NUMERIC(5,2)
        );

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
    """)
    conn.commit()
    print("  ✓ Tables ready\n")


    # ── 2. INSERT CUSTOMERS ───────────────────────────────────────────────────
    print("Inserting customers...")
    customer_ids = []
    for name, ctype, phone, email, address in CUSTOMERS:
        cur.execute("""
            INSERT INTO customers (customer_name, customer_type, contact_number, email, address, registered_on)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """, (name, ctype, phone, email, address, date(random.randint(2010, 2020), random.randint(1, 12), 1)))
        customer_ids.append(cur.fetchone()[0])
    conn.commit()
    print(f"  ✓ {len(customer_ids)} customers inserted\n")


    # ── 3. INSERT VEHICLES ────────────────────────────────────────────────────
    print("Inserting vehicles...")
    vehicle_ids = []
    for v_number, v_type, owner_index, cap in VEHICLES:
        owner_id = customer_ids[owner_index]
        cur.execute("""
            INSERT INTO vehicles (vehicle_number, vehicle_type, owner_customer_id, capacity_tons)
            VALUES (%s, %s, %s, %s)
            RETURNING vehicle_id
        """, (v_number, v_type, owner_id, cap))
        vehicle_ids.append(cur.fetchone()[0])
    conn.commit()
    print(f"  ✓ {len(vehicle_ids)} vehicles inserted\n")


    # ── 4. INSERT LANDFILL MASTER ─────────────────────────────────────────────
    print("Inserting landfill sites...")
    landfill_ids       = []
    landfill_capacities = []
    for site_name, location, region, op_since, capacity in LANDFILLS:
        cur.execute("""
            INSERT INTO landfill_master
                (site_name, location, region, operating_since, licensed_capacity_tons, operator_name, compliance_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING landfill_id
        """, (site_name, location, region, op_since, capacity, "Tamil Nadu Waste Authority", "Compliant"))
        landfill_ids.append(cur.fetchone()[0])
        landfill_capacities.append(capacity)
    conn.commit()
    print(f"  ✓ {len(landfill_ids)} landfill sites inserted\n")


    # ── 5. INSERT TRANSACTIONS ────────────────────────────────────────────────
    print("Inserting transactions (365 days × 5 sites × 50–100 per day)...")

    end_date   = date.today()
    start_date = end_date - timedelta(days=364)
    all_dates  = [start_date + timedelta(days=i) for i in range(365)]

    # Track cumulative weight per landfill for operational status
    cumulative_weight = {lid: 0.0 for lid in landfill_ids}

    total_transactions = 0

    for single_date in all_dates:
        for landfill_id in landfill_ids:
            num_transactions = random.randint(50, 100)

            for _ in range(num_transactions):
                customer_id  = random.choice(customer_ids)
                vehicle_id   = random.choice(vehicle_ids)
                category     = random.choice(WASTE_CATEGORIES)
                weight       = round(random.uniform(1.5, 12.0), 3)
                lo, hi       = CHARGE_RATES[category]
                charge_per_t = round(random.uniform(lo, hi), 2)
                total        = round(weight * charge_per_t, 2)
                payment      = random.choice(PAYMENT_STATUSES)

                cur.execute("""
                    INSERT INTO transactions
                        (landfill_id, customer_id, vehicle_id, transaction_date, transaction_time,
                         waste_category, incoming_weight_tons, charge_per_ton, total_charge, payment_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (landfill_id, customer_id, vehicle_id, single_date, random_time_of_day(),
                      category, weight, charge_per_t, total, payment))

                cumulative_weight[landfill_id] += weight
                total_transactions += 1

        conn.commit()   # commit once per day

    print(f"  ✓ {total_transactions:,} transactions inserted\n")


    # ── 6. UPSERT LANDFILL OPERATIONAL STATUS (end-of-day snapshot) ───────────
    print("Updating landfill operational status...")
    for i, landfill_id in enumerate(landfill_ids):
        capacity  = float(landfill_capacities[i])
        used      = min(cumulative_weight[landfill_id], capacity)
        remaining = round(capacity - used, 2)
        util_pct  = round((used / capacity) * 100, 2)

        cur.execute("""
            INSERT INTO landfill_operational_status
                (landfill_id, last_updated_date, total_capacity_tons,
                 used_capacity_tons, remaining_capacity_tons, utilization_pct)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (landfill_id) DO UPDATE SET
                last_updated_date       = EXCLUDED.last_updated_date,
                used_capacity_tons      = EXCLUDED.used_capacity_tons,
                remaining_capacity_tons = EXCLUDED.remaining_capacity_tons,
                utilization_pct         = EXCLUDED.utilization_pct
        """, (landfill_id, date.today(), capacity, round(used, 2), remaining, util_pct))

    conn.commit()
    print("  ✓ Operational status updated\n")


    # ── 7. INSERT LANDFILL ALERTS (if threshold breached) ─────────────────────
    print("Checking and inserting alerts...")
    alert_count = 0
    for i, landfill_id in enumerate(landfill_ids):
        capacity = float(landfill_capacities[i])
        used     = min(cumulative_weight[landfill_id], capacity)
        util_pct = round((used / capacity) * 100, 2)

        if util_pct >= ALERT_THRESHOLD:
            level, message = get_alert_level(util_pct)
            cur.execute("""
                INSERT INTO landfill_alerts
                    (landfill_id, alert_date, utilization_pct_at_alert,
                     threshold_pct, alert_level, alert_message, is_acknowledged)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (landfill_id, date.today(), util_pct, ALERT_THRESHOLD, level, message, False))
            alert_count += 1
            print(f"  ⚠ Alert [{level}] — Site {landfill_id} at {util_pct}% capacity")

    conn.commit()
    if alert_count == 0:
        print("  ✓ No thresholds breached — no alerts generated")
    print()

    cur.close()
    conn.close()

    print("=" * 55)
    print("✅ All done!")
    print(f"   Customers       : {len(customer_ids)}")
    print(f"   Vehicles        : {len(vehicle_ids)}")
    print(f"   Landfill Sites  : {len(landfill_ids)}")
    print(f"   Transactions    : {total_transactions:,}")
    print(f"   Alerts generated: {alert_count}")
    print("=" * 55)


if __name__ == "__main__":
    main()