import json
import random
import os
from datetime import datetime

# Configuration: Base Prices in ZAR per Ton (Approximate SA Market values)
COMMODITIES = {
    'MZ-YEL': {'name': 'Yellow Maize', 'base_price': 4200.00, 'volatility': 0.05},
    'WH-DUR': {'name': 'Wheat', 'base_price': 6500.00, 'volatility': 0.03},
    'SY-BEA': {'name': 'Soybeans', 'base_price': 9800.00, 'volatility': 0.08},
    'SF-OIL': {'name': 'Sunflower', 'base_price': 8900.00, 'volatility': 0.06},
    'CT-VAL': {'name': 'Citrus (Export)', 'base_price': 15000.00, 'volatility': 0.10}
}

def get_market_prices(date_str):
    """
    Simulates an API response for daily commodity prices.
    Returns a dictionary (JSON structure).
    """
    market_data = {
        "api_source": "JSE_Commodity_Sim",
        "date": date_str,
        "currency": "ZAR",
        "prices": []
    }

    for code, info in COMMODITIES.items():
        # Fluctuate price by random % (Volatility)
        change_percent = random.uniform(-info['volatility'], info['volatility'])
        current_price = info['base_price'] * (1 + change_percent)
        
        market_data["prices"].append({
            "crop_code": code,
            "crop_name": info['name'],
            "price_per_ton": round(current_price, 2),
            "trend": "up" if change_percent > 0 else "down"
        })
    
    return market_data

if __name__ == "__main__":
    # 1. Output Location
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Generate Today's Prices
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"market_prices_{today}.json"
    filepath = os.path.join(output_dir, filename)
    
    # 3. Create Data
    print(f"📈 Fetching market prices for {today}...")
    data = get_market_prices(today)
    
    # 4. Save as JSON
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
        
    print(f"✅ Market Feed Saved: {filepath}")