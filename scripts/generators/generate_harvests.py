import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Configuration: The "Business Truth"
FARMS = [
    {'id': 'F001', 'name': 'Green Valley', 'manager': 'John Doe'},
    {'id': 'F002', 'name': 'Highveld Agri', 'manager': 'Sarah Smith'},
    {'id': 'F003', 'name': 'Cape Vineyards', 'manager': 'David Ngwenya'}
]

CROPS = [
    {'code': 'MZ-YEL', 'name': 'Maize', 'variety': 'Yellow Dent'},
    {'code': 'WH-DUR', 'name': 'Wheat', 'variety': 'Durum'},
    {'code': 'SY-BEA', 'name': 'Soy', 'variety': 'High Protein'},
    {'code': 'SF-OIL', 'name': 'Sunflower', 'variety': 'Oil Seed'},
    {'code': 'CT-VAL', 'name': 'Citrus', 'variety': 'Valencia'}
]

def generate_daily_log(date_str, num_records=20):
    """
    Generates a DataFrame of harvest logs for a specific day.
    """
    data = []
    
    for _ in range(num_records):
        farm = random.choice(FARMS)
        crop = random.choice(CROPS)
        
        # Simulate realistic data
        qty_kg = round(random.uniform(500, 5000), 2)  # Between 500kg and 5 tons
        spoilage_rate = random.uniform(0.01, 0.15)    # 1% to 15% spoilage
        
        # Create the record
        record = {
            'harvest_id': fake.uuid4(),
            'date': date_str,
            'farm_id': farm['id'],
            'farm_name': farm['name'],
            'crop_code': crop['code'],
            'crop_name': crop['name'],
            'qty_harvested_kg': qty_kg,
            'spoilage_kg': round(qty_kg * spoilage_rate, 2),
            'labor_hours': random.randint(4, 12),
            # Introduce some "dirty data" (5% chance of missing manager name)
            'manager_check': farm['manager'] if random.random() > 0.05 else None
        }
        data.append(record)
        
    return pd.DataFrame(data)

if __name__ == "__main__":
    # 1. Define where the data goes
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Simulate "Today's" Upload
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"harvest_log_{today}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # 3. Generate and Save
    print(f"🚜 Generating daily harvest logs for {today}...")
    df = generate_daily_log(today, num_records=50)
    
    df.to_csv(filepath, index=False)
    print(f"✅ File saved: {filepath}")
    print("   Sample Data:")
    print(df.head(3))