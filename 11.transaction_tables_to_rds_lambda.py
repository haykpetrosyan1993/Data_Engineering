import json
import awswrangler as wr
import psycopg2
import psycopg2.extras

def lambda_handler(event, context):
    # TODO implement

    players = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/players.csv', encoding = "ISO-8859-1")
    matches = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/matches.csv', encoding = "ISO-8859-1")

    con = psycopg2.connect(host = "",
                        database = "",
                        user = "",
                        password = "",
                        port = '')

    cur = con.cursor()
    cur.execute("ROLLBACK")
    #------------------------------------------------------------------------players-------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS players(
         id INT PRIMARY KEY NOT NULL,
         player_name VARCHAR(50),
         country_id INT REFERENCES country(id)
         )
         """)
    lst = [tuple(i) for i in players.values]
    try:
       query = """INSERT INTO players (id, player_name, country_id
       ) VALUES (%s,%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('players is done')

    #------------------------------------------------------------------------matches-------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS matches(
         id INT PRIMARY KEY NOT NULL,
         match_date TIMESTAMP,
         home_team_id INT REFERENCES teams(id),
         guest_team_id INT REFERENCES teams(id),
         home_team_goals INT,
         guest_team_goals INT
         )
         """)
    lst = [tuple(i) for i in matches.values]
    try:
       query = """INSERT INTO matches (id, match_date, home_team_id, guest_team_id, home_team_goals, guest_team_goals
       ) VALUES (%s,%s,%s,%s,%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('matches is done')



    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
