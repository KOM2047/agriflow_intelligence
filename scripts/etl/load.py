import pandas as pd
from sqlalchemy import create_engine, text
import os

# ---------------------------------------------------------
# 1. DATABASE CONNECTION
# ---------------------------------------------------------
# Connection String: postgresql://user:password@host:port/database
# Note: When running locally, host is 'localhost'. Inside Docker, it's 'agriflow_db'.
DB_URI = "postgresql://admin:password123@127.0.0.1:5435/agriflow_dw"
engine = create_engine(DB_URI)

# ---------------------------------------------------------
# 2. DIMENSION SEEDING (One-time Setup)
# ---------------------------------------------------------
def seed_dimensions():
    """
    Ensures that dim_crop and dim_farm have data so Foreign Keys work.
    In a real system, these would be loaded by a separate upstream process.
    """
    with engine.connect() as conn:
        # Check if dim_crop is empty
        result = conn.execute(text("SELECT COUNT(*) FROM dim_crop"))
        if result.scalar() == 0:
            print("   [Load] Seeding dim_crop...")
            crops = [
                {'crop_code': 'MZ-YEL', 'crop_name': 'Maize', 'variety': 'Yellow Dent'},
                {'crop_code': 'WH-DUR', 'crop_name': 'Wheat', 'variety': 'Durum'},
                {'crop_code': 'SY-BEA', 'crop_name': 'Soy', 'variety': 'High Protein'},
                {'crop_code': 'SF-OIL', 'crop_name': 'Sunflower', 'variety': 'Oil Seed'},
                {'crop_code': 'CT-VAL', 'crop_name': 'Citrus', 'variety': 'Valencia'}
            ]
            pd.DataFrame(crops).to_sql('dim_crop', engine, if_exists='append', index=False)

        # Check if dim_farm is empty
        result = conn.execute(text("SELECT COUNT(*) FROM dim_farm"))
        if result.scalar() == 0:
            print("   [Load] Seeding dim_farm...")
            farms = [
                {'farm_id': 'F001', 'farm_name': 'Green Valley', 'manager_name': 'John Doe', 'is_current': True, 'valid_from': '2020-01-01'},
                {'farm_id': 'F002', 'farm_name': 'Highveld Agri', 'manager_name': 'Sarah Smith', 'is_current': True, 'valid_from': '2020-01-01'},
                {'farm_id': 'F003', 'farm_name': 'Cape Vineyards', 'manager_name': 'David Ngwenya', 'is_current': True, 'valid_from': '2020-01-01'}
            ]
            pd.DataFrame(farms).to_sql('dim_farm', engine, if_exists='append', index=False)

# ---------------------------------------------------------
# 3. LOADING LOGIC
# ---------------------------------------------------------
def load_to_postgres(df):
    """
    Loads the transformed DataFrame into PostgreSQL.
    Performs Lookups for Foreign Keys (Crop ID, Farm Key).
    """
    try:
        print("   [Load] Connecting to Warehouse...")
        seed_dimensions() # Ensure dims exist
        
        # A. Handle Date Dimension
        df['date_id'] = df['date'].str.replace('-', '').astype(int)
        
        # B. Lookup Crop IDs
        dim_crop = pd.read_sql("SELECT crop_id, crop_code FROM dim_crop", engine)
        df = df.merge(dim_crop, on='crop_code', how='left')
        
        # C. Lookup Farm Keys
        dim_farm = pd.read_sql("SELECT farm_key, farm_id FROM dim_farm WHERE is_current = True", engine)
        df = df.merge(dim_farm, on='farm_id', how='left')
        
        # D. Clean up Columns for Insert
        output_df = df[[
            'date_id', 'crop_id', 'farm_key', 
            'qty_harvested_kg', 'spoilage_kg', 
            'labor_cost_zar', 'logistics_cost_zar', 
            'revenue_zar', 'profit_zar'
        ]].copy()
        
        output_df.rename(columns={'qty_harvested_kg': 'quantity_harvested_kg'}, inplace=True)

        # ---------------------------------------------------------
        # E. Idempotency (THE FIX IS HERE)
        # ---------------------------------------------------------
        # Convert NumPy array to a standard Python List first using .tolist()
        date_list = output_df['date_id'].unique().tolist()
        
        if len(date_list) > 0:
            # Create a string for SQL like (20260217, 20260218)
            # If only 1 item, we format it manually to avoid SQL syntax errors
            if len(date_list) == 1:
                date_tuple_str = f"({date_list[0]})"
            else:
                date_tuple_str = str(tuple(date_list))
            
            delete_query = f"DELETE FROM fact_harvest_yield WHERE date_id IN {date_tuple_str}"
            
            with engine.connect() as conn:
                conn.execute(text(delete_query))
                conn.commit()
                print(f"   [Load] Cleared existing data for dates: {date_tuple_str}")

        # F. Final Insert
        output_df.to_sql('fact_harvest_yield', engine, if_exists='append', index=False)
        print(f"   [Load] ✅ Successfully loaded {len(output_df)} rows into fact_harvest_yield")
        
        return True

    except Exception as e:
        print(f"   [Load] ❌ Error loading to DB: {e}")
        # Print the query that failed to help debug
        import traceback
        traceback.print_exc()
        return False

# ==========================================
# Test Block
# ==========================================
if __name__ == "__main__":
    # Import previous steps to run a full test
    from extract import extract_harvest_data, extract_market_data
    from transform import transform_data
    
    # 1. Simulate the Pipeline Flow
    staging_dir = os.path.join("data", "staging")
    csv_file = next((f for f in os.listdir(staging_dir) if f.endswith('.csv')), None)
    json_file = next((f for f in os.listdir(staging_dir) if f.endswith('.json')), None)
    
    if csv_file and json_file:
        raw_harvest = extract_harvest_data(os.path.join(staging_dir, csv_file))
        raw_market = extract_market_data(os.path.join(staging_dir, json_file))
        
        if raw_harvest is not None and raw_market is not None:
            clean_data = transform_data(raw_harvest, raw_market)
            
            # 2. Run Load
            # NOTE: We need to make sure dim_date has the date key first.
            # Quick hack for the test block:
            date_key = int(clean_data['date'].iloc[0].replace('-', ''))
            with engine.connect() as conn:
                 # Ensure date exists to prevent FK error
                conn.execute(text(f"INSERT INTO dim_date (date_id, full_date) VALUES ({date_key}, '{clean_data['date'].iloc[0]}') ON CONFLICT DO NOTHING"))
                conn.commit()

            load_to_postgres(clean_data)