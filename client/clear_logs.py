# clear_logs.py
import os

BASE_DIR = os.path.dirname(__file__)
LOG_PATH = os.path.join(BASE_DIR, "logs", "client.log")
print(BASE_DIR)
if os.path.exists(LOG_PATH):
    open(LOG_PATH, 'w').close()
    print("Client logs cleared successfully.")
else:
    print("Log file not found.")