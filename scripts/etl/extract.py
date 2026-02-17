import pandas as pd
import json
import os

def extract_harvest_data(filepath):
    """
    Reads a CSV file from the Staging area and returns a Pandas DataFrame.
    """
    try:
        # Read CSV
        df = pd.read_csv(filepath)
        
        # Basic validation: Check if critical columns exist
        required_columns = ['harvest_id', 'date', 'farm_id', 'crop_code', 'qty_harvested_kg']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing columns in {filepath}")
            
        print(f"   [Extract] Successfully loaded {len(df)} harvest records from {os.path.basename(filepath)}")
        return df
    
    except Exception as e:
        print(f"   [Extract] Error reading CSV: {e}")
        return None

def extract_market_data(filepath):
    """
    Reads a JSON file from the Staging area and returns a Pandas DataFrame.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            
        # The JSON structure has a "prices" list inside it
        if "prices" not in data:
            raise ValueError("Invalid JSON format: 'prices' key missing")
            
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(data["prices"])
        
        # Add the date from the JSON metadata as a column for joining later
        df['date'] = data['date']
        
        print(f"   [Extract] Successfully loaded {len(df)} market prices from {os.path.basename(filepath)}")
        return df
        
    except Exception as e:
        print(f"   [Extract] Error reading JSON: {e}")
        return None

# ==========================================
# Quick Test Block (Runs only if you execute this file directly)
# ==========================================
if __name__ == "__main__":
    # Test with the files currently in your staging folder
    staging_dir = os.path.join("data", "staging")
    
    # Find the first CSV and JSON file to test with
    csv_file = next((f for f in os.listdir(staging_dir) if f.endswith('.csv')), None)
    json_file = next((f for f in os.listdir(staging_dir) if f.endswith('.json')), None)
    
    if csv_file:
        extract_harvest_data(os.path.join(staging_dir, csv_file))
    else:
        print("⚠️ No CSV found in Staging to test.")

    if json_file:
        extract_market_data(os.path.join(staging_dir, json_file))
    else:
        print("⚠️ No JSON found in Staging to test.")