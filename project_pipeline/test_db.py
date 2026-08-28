import config

print("Host:", config.DB_HOST)
print("Port:", config.DB_PORT)
print("Database:", config.DB_NAME)
print("User:", config.DB_USER)
print("Password:", config.DB_PASSWORD)

from database.db import Database

try:
    db = Database()
    db.connect()
    print("Database Connected Successfully!")
    db.close()
except Exception as e:
    print(e)