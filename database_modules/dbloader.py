from __future__ import division
import mysql.connector
from dbinitialization import get_environment_vars
import statsapi

if __name__ == "__main__":
    HOST, USER, PASSWORD, DB = get_environment_vars()
    conn = mysql.connector.connect(host=HOST,user=USER,password=PASSWORD,database=DB)
    print("Connected to DB")
    
    dbcursor = conn.cursor()    
    dbcursor.execute("DROP TABLE IF EXISTS players")
    dbcursor.execute("DROP TABLE IF EXISTS positions")
    dbcursor.execute("DROP TABLE IF EXISTS statuses")
    dbcursor.execute("DROP TABLE IF EXISTS teams")
    dbcursor.execute("DROP TABLE IF EXISTS divisions")
    dbcursor.execute("DROP TABLE IF EXISTS leagues")


    dbcursor.execute("CREATE TABLE leagues(leagueId INT, name VARCHAR(15), PRIMARY KEY (leagueId))")
    dbcursor.execute(
        """CREATE TABLE divisions(
            divisionId INT, 
            leagueId INT, 
            name VARCHAR(25), 
            PRIMARY KEY (divisionId), 
            FOREIGN KEY (leagueId) REFERENCES leagues(leagueId) ON DELETE CASCADE ON UPDATE CASCADE
            )
            """)
    dbcursor.execute(
        """CREATE TABLE teams(
            teamId INT, 
            name VARCHAR(30), 
            teamCode VARCHAR(4), 
            fileCode VARCHAR(4),
            abbreviation VARCHAR(4),
            teamName VARCHAR(15),
            locationName VARCHAR(20),
            firstYearOfPlay CHAR(4),
            leagueId INT,
            divisionId INT,
            PRIMARY KEY (teamId),
            FOREIGN KEY (leagueId) REFERENCES leagues(leagueId) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (divisionId) REFERENCES divisions(divisionId) ON DELETE CASCADE ON UPDATE CASCADE
            )
            """)

    team_vals = []
    leagues = {}
    divisions = {}
    teams = statsapi.get("teams", {"leagueIds":103})["teams"]
    for team in teams:
        try:
            leagues[team["league"]["id"]] = team["league"]["name"]
            divisions[team["division"]["id"]] = (team["league"]["id"] ,team["division"]["name"])
            team_vals.append((team["id"], team["name"], team["teamCode"], team["fileCode"], team["abbreviation"], 
            team["teamName"],team["locationName"], team["firstYearOfPlay"], team["league"]["id"],team["division"]["id"]))
        except KeyError as e:
            print("Team", team["name"], "has error", e)

    teams = statsapi.get("teams", {"leagueIds":104})["teams"]
    for team in teams:
        try:
            leagues[team["league"]["id"]] = team["league"]["name"]
            divisions[team["division"]["id"]] = (team["league"]["id"] ,team["division"]["name"])
            team_vals.append((team["id"], team["name"], team["teamCode"], team["fileCode"], team["abbreviation"], 
            team["teamName"],team["locationName"], team["firstYearOfPlay"], team["league"]["id"],team["division"]["id"]))

        except KeyError as e:
            print("Team", team["name"], "has error", e)
    league_vals = []
    for k, v in leagues.items():
        league_vals.append((k,v))
    for i in league_vals:
        print(i)
    division_vals =[]
    for k, v in divisions.items():
        division_vals.append((k,v[0],v[1]))
    for i in division_vals:
        print(i)
    print(team_vals[2])
    print(team_vals[3])
    print(team_vals[4])
    dbcursor.executemany("INSERT INTO leagues(leagueId, name) VALUES (%s,%s)",league_vals)
    conn.commit()
    dbcursor.executemany("INSERT INTO divisions(divisionId, leagueId, name) VALUES (%s,%s,%s)",division_vals)
    conn.commit()
    team_sql = """INSERT INTO teams(teamId, name, teamCode, fileCode, abbreviation, 
    teamName, locationName, firstYearOfPlay, leagueId, divisionId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    dbcursor.executemany(team_sql, team_vals)
    conn.commit()



    dbcursor.execute(
        """CREATE TABLE positions(
            code INT, 
            name VARCHAR(10), 
            type VARCHAR(10), 
            abbreviation VARCHAR(2), 
            PRIMARY KEY (code)
            )""")
    dbcursor.execute(
        """CREATE TABLE statuses(
            code VARCHAR(3),
            description VARCHAR(40),
            PRIMARY KEY (code)
            )""")

    dbcursor.execute(
        """CREATE TABLE players(
            playerId INT, 
            fullName VARCHAR(35), 
            jerseyNumber INT, 
            position INT, 
            status VARCHAR(3), 
            teamID INT,
            PRIMARY KEY (playerId),
            FOREIGN KEY (position) REFERENCES positions(code),
            FOREIGN KEY (teamID) REFERENCES teams(teamId),
            FOREIGN KEY (status) REFERENCES statuses(code)
            )""")

    dbcursor.execute("""SELECT teamId FROM teams""")
    ids = []
    for i in dbcursor.fetchall():
        ids.append(i[0])

    positions = {}
    statuses = {}
    players = []
    for i in ids:
        # i is team id, make api call and then iterate over the result inserting into the db
        resp = statsapi.get("team_roster", {"teamId":i})["roster"]
        for elem in resp:
            try:
                j = elem["person"]
                positions[j["position"]["code"]] = (j["position"]["name"], j["position"]["type"], j["position"]["abbreviation"])
                statuses[j["status"]["code"]] = (j["status"]["description"])
                players.append((j["id"],j["fullName"],j["jerseyNumber"],j["position"]["code"],j["status"]["code"],j["parentTeamId"]))
            except KeyError:
                print(j)

    position_sql = """INSERT INTO positions VALUES (%s, %s, %s, %s)"""
    position_val = [(k, v[0], v[1], v[2]) for k,v in positions.items()]

    dbcursor.executemany(position_sql, position_val)
    conn.commit()
    
    statuses_sql = """INSERT INTO statuses VALUES (%s,%s)"""
    statuses_val = [(k,v[0]) for k,v in statuses.items()]

    dbcursor.executemany(statuses_sql, statuses_val)
    conn.commit()

    player_sql = """INSERT INTO players VALUES (%s,%s,%s,%s,%s,%s)"""
    dbcursor.executemany(player_sql, players)
    conn.commit()





