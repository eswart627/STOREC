import uuid
import time
from .connection import get_connection

def insert_fake_datanodes():
    """Insert 10 fake datanodes into the dn_table"""
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Generate 10 fake datanodes
    for i in range(10):
        node_id = str(uuid.uuid4())
        hostname = f"192.168.1.{100 + i}"  # IPs from 192.168.1.100 to 192.168.1.109
        port = 50052 + i  # Ports from 50052 to 50061
        status = "INACTIVE"
        last_heartbeat = int(time.time())  # Current timestamp
        capacity = 1000000000  # 1GB in bytes
        used = 0
        available = capacity
        
        cur.execute("""
            INSERT INTO dn_table (
                dn_id, dn_address, dn_port, dn_status,
                dn_last_heartbeat, dn_capacity, dn_used, dn_available
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (node_id, hostname, port, status, last_heartbeat, capacity, used, available))
    
    conn.commit()
    conn.close()
    print(f"Inserted 10 fake datanodes into dn_table")

if __name__ == "__main__":
    insert_fake_datanodes()