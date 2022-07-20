from dbinitialization import get_environment_vars
import mysql.connector

def print_results(dbcursor):
    for i in dbcursor.fetchall():
        print(i)

if __name__ == "__main__":
    HOST, USER, PASSWORD, DB = get_environment_vars()
    conn = mysql.connector.connect(host=HOST,user=USER,password=PASSWORD,database=DB)
    print("Connected to DB")

    dbcursor = conn.cursor()

    print("Query team names that have loctiond different from where they play")
    dbcursor.execute("SELECT T.name FROM teams T WHERE T.name NOT LIKE concat(T.locationName,'%')")
    print_results(dbcursor)

    print("Query the 10 most common jersey numbers")
    dbcursor.execute(
        """SELECT p.jerseyNumber, COUNT(*) 
        FROM players P 
        GROUP BY p.jerseyNumber 
        ORDER BY COUNT(*)
        LIMIT 10
        """)
    print_results(dbcursor)

    print("Query team with the highest average jersey number")
    dbcursor.execute(
        """ SELECT T.name
            FROM teams T, players P
            WHERE P.teamId = T.teamId
            GROUP BY T.name
            ORDER BY AVG(p.jerseyNumber) DESC
            LIMIT 1
            """
    )
    print_results(dbcursor)

    print("Query team with the highest average jersey number and the 3 highest numbers on the team")
    dbcursor.execute(
        """ WITH t1 AS (
            SELECT T.name
            FROM teams T, players P
            WHERE P.teamId = T.teamId
            GROUP BY T.name
            ORDER BY AVG(p.jerseyNumber) DESC
            LIMIT 1
        )   SELECT T.name, P.jerseyNumber
            FROM players P, teams T, t1
            WHERE P.teamId = T.teamId AND T.name = t1.name
            ORDER BY P.jerseyNumber DESC
            LIMIT 3
        """)
    print_results(dbcursor)

    print("Query the team with the least pitchers and the number of pitchers")
    dbcursor.execute(
        """ SELECT T.name, COUNT(*)
            FROM players P, teams T, positions L
            WHERE P.teamId = T.teamId AND L.code = P.position
            AND (L.code = '1' OR L.code = 'Y')
            GROUP BY T.name
            ORDER BY COUNT(*) ASC
            LIMIT 1
        """)
    print_results(dbcursor)

