from .connection import get_connection
import os

try:
    conn = get_connection()
    cur = conn.cursor()
    
    # Correct path to the SQL file
    sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'dn_table.sql')
    
    with open(sql_file_path, "r") as f:
        sql_script = f.read()
    
    cur.execute(sql_script)
    conn.commit()
    print("Database initialized successfully!")
    
except FileNotFoundError:
    print(f"Error: SQL file not found at {sql_file_path}")
except Exception as e:
    print(f"Error initializing database: {e}")
finally:
    if 'conn' in locals():
        conn.close()