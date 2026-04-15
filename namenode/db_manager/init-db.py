import os

from .connection import get_connection


def main() -> None:
    """
    Initialize the database by executing the SQL scripts.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # List of SQL files to execute
        sql_files = ["dn_table.sql", "filetable.sql", "metadata.sql"]
        
        for sql_file in sql_files:
            sql_file_path = os.path.join(
                os.path.dirname(__file__), "..", "models", sql_file
            )

            with open(sql_file_path, "r", encoding="utf-8") as file:
                sql_script = file.read()

            cur.execute(sql_script)
            print(f"Executed {sql_file} successfully!")

        conn.commit()
        print("Database initialized successfully!")

    except FileNotFoundError:
        print(f"Error: SQL file not found")
    except Exception as exc:
        import sys
        print(f"Error initializing database: {exc}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
