import sys
import os

# This line ensures Python can find the `db` folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connections import DuckDBConnection, SnowflakeConnection

def test_connection():
    try:
        print("\n[TEST] Waking up Database Managers...")
        
        # Test 1: DuckDB (Local)
        print("[TEST 1] Testing DuckDB Local Connection...")
        duck_db = DuckDBConnection()
        duck_db.conn.execute("CREATE TABLE IF NOT EXISTS test_duck (id INT)")
        print("         ✅ DuckDB is running perfectly with its local file!")
        
        # Test 2: Snowflake (Cloud)
        print("\n[TEST 2] Testing Snowflake Cloud Connection... (This may take 2 seconds)")
        snow_db = SnowflakeConnection()
        cursor = snow_db.conn.cursor()
        
        # We ask Snowflake for its version, your role, and the warehouse it used!
        cursor.execute("SELECT current_version(), current_role(), current_warehouse()")
        result = cursor.fetchone()
        
        print("\n🎉 BOOM! CONNECTION SUCCESSFUL! 🎉")
        print(f"❄️ Snowflake Version: {result[0]}")
        print(f"❄️ Logged in Role: {result[1]}")
        print(f"❄️ Active Warehouse: {result[2]}\n")
        
    except Exception as e:
        print("\n❌ CONNECTION FAILED! ❌")
        print(f"Error Details:\n{e}\n")
        print("TIP: If you see 'Failed to connect', double-check your .env file for typos!")

if __name__ == "__main__":
    test_connection()
