from .connection import get_connection

def check_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dn_table")
    print(cur.fetchall())
    conn.close()

if __name__ == "__main__":
    check_tables()
