"""
Installation Test Script

Tests basic functionality of the AI Data Analyst components
to ensure everything is installed and configured correctly.
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_imports():
    """Test that all required modules can be imported"""
    
    print("Testing imports...")
    
    try:
        import pandas as pd
        print("✓ pandas imported successfully")
    except ImportError as e:
        print(f"✗ pandas import failed: {e}")
        return False
    
    try:
        import numpy as np
        print("✓ numpy imported successfully")
    except ImportError as e:
        print(f"✗ numpy import failed: {e}")
        return False
    
    try:
        import matplotlib.pyplot as plt
        print("✓ matplotlib imported successfully")
    except ImportError as e:
        print(f"✗ matplotlib import failed: {e}")
        return False
    
    try:
        import plotly.graph_objects as go
        print("✓ plotly imported successfully")
    except ImportError as e:
        print(f"✗ plotly import failed: {e}")
        return False
    
    try:
        import sqlalchemy
        print("✓ sqlalchemy imported successfully")
    except ImportError as e:
        print(f"✗ sqlalchemy import failed: {e}")
        return False
    
    try:
        from sklearn.ensemble import IsolationForest
        print("✓ scikit-learn imported successfully")
    except ImportError as e:
        print(f"✗ scikit-learn import failed: {e}")
        return False
    
    return True

def test_config():
    """Test configuration loading"""
    
    print("\nTesting configuration...")
    
    try:
        from utils.config_manager import ConfigManager
        
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        if not config_path.exists():
            print(f"✗ Configuration file not found: {config_path}")
            return False
        
        config = ConfigManager(str(config_path))
        
        # Test basic config access
        server_host = config.get("server.host", "localhost")
        server_port = config.get("server.port", 8000)
        
        print(f"✓ Configuration loaded successfully")
        print(f"  Server: {server_host}:{server_port}")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_data_generation():
    """Test sample data generation"""
    
    print("\nTesting data generation...")
    
    try:
        # Import the data generation script
        sys.path.append(str(Path(__file__).parent))
        from generate_sample_data import generate_sales_data
        
        # Generate small sample
        df = generate_sales_data(10)
        
        if len(df) != 10:
            print(f"✗ Expected 10 records, got {len(df)}")
            return False
        
        expected_columns = ["transaction_id", "date", "customer_id", "product_name", "total_amount"]
        for col in expected_columns:
            if col not in df.columns:
                print(f"✗ Missing expected column: {col}")
                return False
        
        print("✓ Sample data generation working")
        print(f"  Generated {len(df)} records with {len(df.columns)} columns")
        
        return True
        
    except Exception as e:
        print(f"✗ Data generation test failed: {e}")
        return False

def test_database_creation():
    """Test SQLite database creation"""
    
    print("\nTesting database creation...")
    
    try:
        import sqlite3
        import tempfile
        
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        conn = sqlite3.connect(db_path)
        
        # Create simple test table
        conn.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)
        
        # Insert test data
        conn.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test", 123.45))
        conn.commit()
        
        # Query data
        cursor = conn.execute("SELECT * FROM test_table")
        rows = cursor.fetchall()
        
        conn.close()
        
        # Clean up
        os.unlink(db_path)
        
        if len(rows) != 1:
            print(f"✗ Expected 1 row, got {len(rows)}")
            return False
        
        print("✓ SQLite database operations working")
        
        return True
        
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        return False

def test_file_operations():
    """Test file reading operations"""
    
    print("\nTesting file operations...")
    
    try:
        import pandas as pd
        import tempfile
        import json
        
        # Test CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            csv_path = tmp.name
            tmp.write("name,age,city\nJohn,30,NYC\nJane,25,LA\n")
        
        df_csv = pd.read_csv(csv_path)
        os.unlink(csv_path)
        
        if len(df_csv) != 2 or len(df_csv.columns) != 3:
            print(f"✗ CSV reading failed")
            return False
        
        # Test JSON
        test_data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json_path = tmp.name
            json.dump(test_data, tmp)
        
        with open(json_path, 'r') as f:
            loaded_data = json.load(f)
        
        os.unlink(json_path)
        
        if len(loaded_data) != 2:
            print(f"✗ JSON reading failed")
            return False
        
        print("✓ File operations working")
        
        return True
        
    except Exception as e:
        print(f"✗ File operations test failed: {e}")
        return False

def test_visualization():
    """Test basic visualization creation"""
    
    print("\nTesting visualization...")
    
    try:
        import matplotlib.pyplot as plt
        import plotly.graph_objects as go
        import numpy as np
        
        # Test matplotlib
        fig, ax = plt.subplots()
        x = np.linspace(0, 10, 100)
        y = np.sin(x)
        ax.plot(x, y)
        plt.close(fig)
        
        # Test plotly
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=[1, 2, 3, 4], y=[10, 11, 12, 13]))
        
        print("✓ Visualization libraries working")
        
        return True
        
    except Exception as e:
        print(f"✗ Visualization test failed: {e}")
        return False

def main():
    """Run all tests"""
    
    print("=== AI Data Analyst Installation Test ===\n")
    
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_config),
        ("Data Generation", test_data_generation),
        ("Database Operations", test_database_creation),
        ("File Operations", test_file_operations),
        ("Visualization", test_visualization)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"Test '{test_name}' failed")
        except Exception as e:
            print(f"Test '{test_name}' crashed: {e}")
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All tests passed! The installation is working correctly.")
        print("\nNext steps:")
        print("1. Run 'python scripts/generate_sample_data.py' to create sample data")
        print("2. Copy 'env.template' to '.env' and configure your API keys")
        print("3. Run 'python src/main.py' to start the MCP server")
        return True
    else:
        print("✗ Some tests failed. Please check the installation.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 