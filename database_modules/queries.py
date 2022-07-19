from dbinitialization import get_environment_vars
import mysql.connector

if __name__ == "__main__":
    HOST, USER, PASSWORD, DB = get_environment_vars()
    conn = mysql.connector.connect(host=HOST,user=USER,password=PASSWORD,database=DB)
    print("Connected to DB")

    dbcursor = conn.cursor()

    dbcursor.execute("SELECT T.name FROM teams T WHERE T.name NOT LIKE concat(T.locationName,'%')")
    for i in dbcursor.fetchall():
        print(i)

                                                            