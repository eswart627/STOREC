from .connection import get_connection


def test_connection():
    """
    Test the database connection.
    """
    try:
        conn = get_connection()
        print("Connection successful!")
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
