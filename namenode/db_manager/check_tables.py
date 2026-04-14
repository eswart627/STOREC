from .connection import get_connection

def check_tables():
    """
    Check all tables in the database
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dn_table")
    for row in cur.fetchall():
        print(row)
    cur.execute("SELECT * FROM file_table")
    for row in cur.fetchall():
        print(row)
    cur.execute("SELECT * FROM metadata")
    for row in cur.fetchall():
        print(row)
    conn.close()

if __name__ == "__main__":
    check_tables()
