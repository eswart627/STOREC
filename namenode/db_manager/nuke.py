from .connection import get_connection

def clear_tables() -> None:
    """
    Clear all three tables from the MySQL DB.
    """

    conn = get_connection()
    cur = conn.cursor()

    # Clear the dn_table
    cur.execute("DELETE FROM dn_table")

    # Clear the files_table
    cur.execute("DELETE FROM file_table")

    # Clear the blocks_table
    cur.execute("DELETE FROM metadata_table")
    
    print("Successfully cleared tables.")

    conn.commit()
    conn.close()


# Run the clear_tables function
if __name__=="__main__":
    clear_tables()
