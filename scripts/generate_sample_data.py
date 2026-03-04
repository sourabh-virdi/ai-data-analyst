"""
Sample Data Generation Script

Generates synthetic datasets for testing the AI Data Analyst including
sales data, customer data, financial data, and operational metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import json
from pathlib import Path
import random

# Set random seed for reproducible data
np.random.seed(42)
random.seed(42)

def generate_sales_data(num_records: int = 1000) -> pd.DataFrame:
    """Generate synthetic sales data"""
    
    # Date range: last 2 years
    start_date = datetime.now() - timedelta(days=730)
    dates = [start_date + timedelta(days=x) for x in range(730)]
    
    # Product data
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Tool Pro", "Tool Lite"]
    categories = ["Electronics", "Tools", "Accessories"]
    
    # Customer data
    customers = [f"Customer_{i:03d}" for i in range(1, 101)]
    
    # Regions
    regions = ["North", "South", "East", "West", "Central"]
    
    # Sales representatives
    sales_reps = ["Alice Johnson", "Bob Smith", "Carol Davis", "David Wilson", "Eva Brown"]
    
    data = []
    
    for _ in range(num_records):
        # Generate realistic sales patterns
        base_amount = np.random.lognormal(mean=6, sigma=1)  # Log-normal distribution for realistic sales amounts
        
        # Add seasonal effects (higher sales in Q4)
        date = random.choice(dates)
        if date.month in [11, 12]:  # Holiday season boost
            base_amount *= 1.3
        elif date.month in [1, 2]:  # Post-holiday drop
            base_amount *= 0.8
        
        # Weekend effect (lower sales)
        if date.weekday() >= 5:
            base_amount *= 0.7
        
        product = random.choice(products)
        category = random.choice(categories)
        
        # Product-specific pricing
        if "Pro" in product:
            base_amount *= 1.5
        elif "Lite" in product:
            base_amount *= 0.8
        
        quantity = max(1, int(np.random.poisson(3)))  # Poisson distribution for quantities
        unit_price = base_amount / quantity
        
        record = {
            "transaction_id": f"TXN_{random.randint(100000, 999999)}",
            "date": date.strftime("%Y-%m-%d"),
            "customer_id": random.choice(customers),
            "product_name": product,
            "category": category,
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "total_amount": round(base_amount, 2),
            "region": random.choice(regions),
            "sales_rep": random.choice(sales_reps),
            "quarter": f"Q{(date.month - 1) // 3 + 1}",
            "month": date.strftime("%B"),
            "year": date.year,
            "discount_percent": round(random.uniform(0, 20), 1) if random.random() < 0.3 else 0
        }
        
        data.append(record)
    
    return pd.DataFrame(data)

def generate_customer_data(num_customers: int = 100) -> pd.DataFrame:
    """Generate synthetic customer data"""
    
    # Customer segments
    segments = ["Enterprise", "SMB", "Startup", "Individual"]
    industries = ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail", "Education"]
    
    data = []
    
    for i in range(1, num_customers + 1):
        # Generate realistic customer lifetime values
        segment = random.choice(segments)
        
        if segment == "Enterprise":
            base_ltv = np.random.lognormal(mean=10, sigma=0.5)
        elif segment == "SMB":
            base_ltv = np.random.lognormal(mean=8, sigma=0.5)
        elif segment == "Startup":
            base_ltv = np.random.lognormal(mean=7, sigma=0.7)
        else:  # Individual
            base_ltv = np.random.lognormal(mean=6, sigma=0.8)
        
        # Registration date (last 3 years)
        reg_date = datetime.now() - timedelta(days=random.randint(1, 1095))
        
        # Last purchase date
        days_since_last = np.random.exponential(30)  # Exponential distribution
        last_purchase = datetime.now() - timedelta(days=int(days_since_last))
        
        record = {
            "customer_id": f"Customer_{i:03d}",
            "customer_name": f"Company {chr(64 + i)} Inc." if segment != "Individual" else f"John Doe {i}",
            "segment": segment,
            "industry": random.choice(industries),
            "registration_date": reg_date.strftime("%Y-%m-%d"),
            "last_purchase_date": last_purchase.strftime("%Y-%m-%d"),
            "lifetime_value": round(base_ltv, 2),
            "total_orders": max(1, int(np.random.poisson(8))),
            "avg_order_value": round(base_ltv / max(1, int(np.random.poisson(8))), 2),
            "is_active": random.random() > 0.15,  # 85% active customers
            "credit_limit": round(base_ltv * random.uniform(1.2, 2.0), 2),
            "region": random.choice(["North", "South", "East", "West", "Central"]),
            "contact_email": f"contact{i}@company{i}.com",
            "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        }
        
        data.append(record)
    
    return pd.DataFrame(data)

def generate_financial_data(num_records: int = 365) -> pd.DataFrame:
    """Generate synthetic financial data (daily for 1 year)"""
    
    start_date = datetime.now() - timedelta(days=365)
    
    data = []
    
    # Running totals for realistic cash flow
    cash_balance = 100000
    
    for i in range(num_records):
        date = start_date + timedelta(days=i)
        
        # Generate realistic financial metrics
        daily_revenue = max(0, np.random.normal(5000, 1500))
        daily_expenses = max(0, np.random.normal(3500, 1000))
        
        # Seasonal effects
        if date.month in [11, 12]:  # Holiday season
            daily_revenue *= 1.4
            daily_expenses *= 1.2
        elif date.month in [1, 2]:  # Slow season
            daily_revenue *= 0.8
            daily_expenses *= 0.9
        
        # Weekend effects
        if date.weekday() >= 5:
            daily_revenue *= 0.3
            daily_expenses *= 0.5
        
        # Update cash balance
        cash_flow = daily_revenue - daily_expenses
        cash_balance += cash_flow
        
        # Profit margin
        profit_margin = (daily_revenue - daily_expenses) / daily_revenue * 100 if daily_revenue > 0 else 0
        
        record = {
            "date": date.strftime("%Y-%m-%d"),
            "revenue": round(daily_revenue, 2),
            "expenses": round(daily_expenses, 2),
            "profit": round(daily_revenue - daily_expenses, 2),
            "cash_flow": round(cash_flow, 2),
            "cash_balance": round(cash_balance, 2),
            "profit_margin": round(profit_margin, 2),
            "accounts_receivable": round(daily_revenue * random.uniform(0.1, 0.3), 2),
            "accounts_payable": round(daily_expenses * random.uniform(0.1, 0.25), 2),
            "inventory_value": round(np.random.normal(25000, 5000), 2),
            "quarter": f"Q{(date.month - 1) // 3 + 1}",
            "month": date.strftime("%B"),
            "year": date.year
        }
        
        data.append(record)
    
    return pd.DataFrame(data)

def generate_user_activity_data(num_records: int = 500) -> pd.DataFrame:
    """Generate synthetic user activity data"""
    
    start_date = datetime.now() - timedelta(days=365)
    
    data = []
    
    for i in range(num_records):
        date = start_date + timedelta(days=random.randint(0, 365))
        
        # Generate realistic user metrics
        base_users = 1000
        
        # Growth trend
        days_from_start = (date - start_date).days
        growth_factor = 1 + (days_from_start / 365) * 0.5  # 50% annual growth
        
        # Seasonal patterns
        if date.month in [6, 7, 8]:  # Summer dip
            seasonal_factor = 0.8
        elif date.month in [11, 12]:  # Holiday boost
            seasonal_factor = 1.3
        else:
            seasonal_factor = 1.0
        
        # Weekend patterns
        if date.weekday() >= 5:
            weekend_factor = 1.4  # Higher weekend usage
        else:
            weekend_factor = 1.0
        
        daily_active_users = int(base_users * growth_factor * seasonal_factor * weekend_factor * random.uniform(0.8, 1.2))
        monthly_active_users = int(daily_active_users * random.uniform(8, 12))
        
        record = {
            "date": date.strftime("%Y-%m-%d"),
            "daily_active_users": daily_active_users,
            "monthly_active_users": monthly_active_users,
            "new_signups": max(0, int(np.random.poisson(daily_active_users * 0.02))),
            "session_duration_avg": round(random.uniform(5, 45), 2),
            "page_views": int(daily_active_users * random.uniform(3, 8)),
            "bounce_rate": round(random.uniform(20, 60), 2),
            "conversion_rate": round(random.uniform(1, 8), 2),
            "revenue_per_user": round(random.uniform(0.5, 15), 2),
            "churn_rate": round(random.uniform(2, 8), 2),
            "quarter": f"Q{(date.month - 1) // 3 + 1}",
            "month": date.strftime("%B"),
            "year": date.year
        }
        
        data.append(record)
    
    return pd.DataFrame(data)

def create_database_and_tables():
    """Create SQLite database with sample data"""
    
    db_path = Path("data/sample.db")
    db_path.parent.mkdir(exist_ok=True)
    
    # Generate datasets
    print("Generating sales data...")
    sales_df = generate_sales_data(1000)
    
    print("Generating customer data...")
    customers_df = generate_customer_data(100)
    
    print("Generating financial data...")
    financial_df = generate_financial_data(365)
    
    print("Generating user activity data...")
    activity_df = generate_user_activity_data(500)
    
    # Create SQLite database
    print("Creating SQLite database...")
    conn = sqlite3.connect(db_path)
    
    # Insert data into tables
    sales_df.to_sql('sales', conn, if_exists='replace', index=False)
    customers_df.to_sql('customers', conn, if_exists='replace', index=False)
    financial_df.to_sql('financials', conn, if_exists='replace', index=False)
    activity_df.to_sql('user_activity', conn, if_exists='replace', index=False)
    
    conn.close()
    
    print(f"Database created at {db_path}")
    
    # Also save as CSV files
    csv_dir = Path("data/csv")
    csv_dir.mkdir(exist_ok=True)
    
    sales_df.to_csv(csv_dir / "sales_data.csv", index=False)
    customers_df.to_csv(csv_dir / "customer_data.csv", index=False)
    financial_df.to_csv(csv_dir / "financial_data.csv", index=False)
    activity_df.to_csv(csv_dir / "user_activity_data.csv", index=False)
    
    print(f"CSV files saved to {csv_dir}")
    
    # Save as JSON files
    json_dir = Path("data/json")
    json_dir.mkdir(exist_ok=True)
    
    sales_df.to_json(json_dir / "sales_data.json", orient="records", indent=2)
    customers_df.to_json(json_dir / "customer_data.json", orient="records", indent=2)
    
    print(f"JSON files saved to {json_dir}")
    
    # Create data summary
    summary = {
        "datasets": {
            "sales": {
                "records": len(sales_df),
                "columns": list(sales_df.columns),
                "date_range": f"{sales_df['date'].min()} to {sales_df['date'].max()}",
                "total_revenue": float(sales_df['total_amount'].sum())
            },
            "customers": {
                "records": len(customers_df),
                "columns": list(customers_df.columns),
                "segments": customers_df['segment'].value_counts().to_dict(),
                "total_ltv": float(customers_df['lifetime_value'].sum())
            },
            "financials": {
                "records": len(financial_df),
                "columns": list(financial_df.columns),
                "date_range": f"{financial_df['date'].min()} to {financial_df['date'].max()}",
                "total_profit": float(financial_df['profit'].sum())
            },
            "user_activity": {
                "records": len(activity_df),
                "columns": list(activity_df.columns),
                "date_range": f"{activity_df['date'].min()} to {activity_df['date'].max()}",
                "avg_daily_users": float(activity_df['daily_active_users'].mean())
            }
        },
        "generated_at": datetime.now().isoformat()
    }
    
    with open("data/data_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print("Data generation complete!")
    print(f"Summary saved to data/data_summary.json")
    
    return summary

if __name__ == "__main__":
    summary = create_database_and_tables()
    
    print("\n=== Data Summary ===")
    for dataset_name, info in summary["datasets"].items():
        print(f"\n{dataset_name.upper()}:")
        print(f"  Records: {info['records']}")
        print(f"  Columns: {len(info['columns'])}")
        if 'date_range' in info:
            print(f"  Date Range: {info['date_range']}")
        if 'total_revenue' in info:
            print(f"  Total Revenue: ${info['total_revenue']:,.2f}")
        if 'total_ltv' in info:
            print(f"  Total Customer LTV: ${info['total_ltv']:,.2f}")
        if 'total_profit' in info:
            print(f"  Total Profit: ${info['total_profit']:,.2f}")
    
    print(f"\nGenerated at: {summary['generated_at']}")
    print("\nYou can now run the MCP server and analyze this data!") 