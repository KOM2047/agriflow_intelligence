import pandas as pd

def transform_data(harvest_df, market_df):
    """
    Applies business logic to raw data:
    1. Cleans missing values.
    2. Joins Harvests with Market Prices.
    3. Calculates Financial Metrics (Revenue, Profit).
    """
    try:
        print("   [Transform] Starting transformation...")

        # ---------------------------------------------------------
        # 1. CLEANING
        # ---------------------------------------------------------
        # Fill missing 'manager_check' with 'Unknown'
        harvest_df['manager_check'] = harvest_df['manager_check'].fillna('Unknown')
        
        # Ensure quantities are positive (Data Quality Check)
        harvest_df = harvest_df[harvest_df['qty_harvested_kg'] > 0]

        # ---------------------------------------------------------
        # 2. ENRICHMENT (The JOIN)
        # ---------------------------------------------------------
        # We need to join on 'crop_code'. 
        # Note: In a real scenario, we'd also join on 'date', but for this 
        # simulation, we assume the market file corresponds to the harvest date.
        
        # Rename market columns to avoid collisions if necessary
        market_clean = market_df[['crop_code', 'price_per_ton', 'crop_name']]
        
        # Perform the Merge (Left Join: Keep all harvests, find matching prices)
        merged_df = pd.merge(
            harvest_df, 
            market_clean, 
            on='crop_code', 
            how='left', 
            suffixes=('', '_market')
        )

        # ---------------------------------------------------------
        # 3. CALCULATIONS (Business Logic)
        # ---------------------------------------------------------
        # Convert Price per Ton to Price per KG
        merged_df['price_per_kg'] = merged_df['price_per_ton'] / 1000.0
        
        # Calculate Revenue (in ZAR)
        # Formula: (Harvested - Spoilage) * Price
        merged_df['revenue_zar'] = (
            (merged_df['qty_harvested_kg'] - merged_df['spoilage_kg']) * merged_df['price_per_kg']
        ).round(2)
        
        # Calculate Costs (Simulated logic for demo purposes)
        # Labor: R150 per hour
        merged_df['labor_cost_zar'] = (merged_df['labor_hours'] * 150.00).round(2)
        
        # Logistics: R2.50 per kg transport cost
        merged_df['logistics_cost_zar'] = (merged_df['qty_harvested_kg'] * 2.50).round(2)
        
        # Calculate Net Profit
        merged_df['profit_zar'] = (
            merged_df['revenue_zar'] - 
            (merged_df['labor_cost_zar'] + merged_df['logistics_cost_zar'])
        ).round(2)

        # ---------------------------------------------------------
        # 4. FINAL FORMATTING
        # ---------------------------------------------------------
        # Select only the columns needed for the Data Warehouse
        final_df = merged_df[[
            'harvest_id', 'date', 'farm_id', 'crop_code', 
            'qty_harvested_kg', 'spoilage_kg', 
            'labor_cost_zar', 'logistics_cost_zar', 
            'revenue_zar', 'profit_zar'
        ]]

        print(f"   [Transform] Successfully transformed {len(final_df)} records.")
        print(f"   [Transform] Total Revenue Calculated: R {final_df['revenue_zar'].sum():,.2f}")
        
        return final_df

    except Exception as e:
        print(f"   [Transform] Error during transformation: {e}")
        return None

# ==========================================
# Quick Test Block
# ==========================================
if __name__ == "__main__":
    # Import your extractors to test the flow
    from extract import extract_harvest_data, extract_market_data
    import os

    # Setup paths
    staging_dir = os.path.join("data", "staging")
    csv_file = next((f for f in os.listdir(staging_dir) if f.endswith('.csv')), None)
    json_file = next((f for f in os.listdir(staging_dir) if f.endswith('.json')), None)

    if csv_file and json_file:
        # 1. Extract
        df_h = extract_harvest_data(os.path.join(staging_dir, csv_file))
        df_m = extract_market_data(os.path.join(staging_dir, json_file))
        
        # 2. Transform
        if df_h is not None and df_m is not None:
            df_final = transform_data(df_h, df_m)
            print("\nPreview of Final Data:")
            print(df_final.head())