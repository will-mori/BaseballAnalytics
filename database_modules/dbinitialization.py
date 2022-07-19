import mysql.connector
from dotenv import load_dotenv
import os

def get_environment_vars():
    load_dotenv()
    HOST = os.getenv("MYSQL_HOST")
    USER = os.getenv("MYSQL_USER")
    PASSWORD = os.getenv("MYSQL_PASSWORD")
    DB = os.getenv("MYSQL_DB")

    return HOST, USER, PASSWORD, DB


if __name__ == "__main__":
    load_dotenv()
    DB = os.getenv("MYSQL_DB")
    if DB is None:
        with open(".env", "a") as f:
            f.write("MYSQL_DB=baseball")
        

    HOST, USER, PASSWORD, DB = get_environment_vars()

    conn = mysql.connector.connect(host=HOST,user=USER,password=PASSWORD)
    print("Connected to DB")

    dbcursor = conn.cursor()
    dbcursor.execute("CREATE DATABASE baseball")
    dbcursor.execute("USE baseball")




