import os

from .connection import get_connection


def main() -> None:
    """Recreate the DataNode table so local development starts from a clean DB."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql_file_path = os.path.join(
            os.path.dirname(__file__), "..", "models", "dn_table.sql"
        )

        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_script = file.read()

        # Drop the table first so stale test IDs do not survive between runs.
        cur.execute("DROP TABLE IF EXISTS dn_table")
        cur.execute(sql_script)
        conn.commit()
        print("Database initialized successfully!")

    except FileNotFoundError:
        print(f"Error: SQL file not found at {sql_file_path}")
    except Exception as exc:
        print(f"Error initializing database: {exc}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
