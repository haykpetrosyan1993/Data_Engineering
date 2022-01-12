import json
import awswrangler as wr
import psycopg2
import psycopg2.extras

def lambda_handler(event, context):
    # TODO implement

    # match_details_2020 = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/match_details _2020.csv', encoding = "ISO-8859-1")
    match_details_2021 = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/match_details_2021.csv', encoding = "ISO-8859-1")
    # player_details_2020 = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/player_details_2020.csv', encoding = "ISO-8859-1")
    player_details_2021 = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/player_details_2021.csv', encoding = "ISO-8859-1")

    con = psycopg2.connect(host = "",
                        database = "",
                        user = "",
                        password = "",
                        port = '')

    cur = con.cursor()
    cur.execute("ROLLBACK")
    # #------------------------------------------------------------------------match_details-------------------------------------------------------------------------


    cur.execute("""
     CREATE TABLE IF NOT EXISTS match_details(
          id INT PRIMARY KEY NOT NULL,
          match_id INT REFERENCES matches(id),
          player_id INT REFERENCES players(id),
          h_team_id INT REFERENCES teams(id),
          a_team_id INT REFERENCES teams(id),
          minute INT,
          result_id INT REFERENCES results(id),
          season_id INT REFERENCES seasons(id),
          shotType_id INT REFERENCES shotstype(id),
          h_a INT REFERENCES home_away(id),
          situation_id INT REFERENCES situations(id)
          )
          """)

    lst = [tuple(i) for i in match_details_2021.values]

    query = """INSERT INTO match_details (id, match_id, player_id, h_team_id, a_team_id, minute, result_id, season_id, shotType_id, h_a, situation_id
       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)


    # #------------------------------------------------------------------------player_details-------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS players_results(
         id INT PRIMARY KEY NOT NULL,
          goals INT,
          own_goals INT,
          shots INT,
          time INT,
          player_id INT REFERENCES players(id),
          team_id INT REFERENCES teams(id),
          position_id INT REFERENCES positions(id),
          h_a INT REFERENCES home_away(id),
          yellow_card INT,
          red_card INT,
          key_passes INT,
          assists INT,
          match_id INT REFERENCES matches(id)
          )
          """)
    lst = [tuple(i) for i in player_details_2021.itertuples(index=False)]

    query = """INSERT INTO players_results (id, goals, own_goals, shots, time, player_id, team_id, position_id, h_a, yellow_card, red_card, key_passes,
        assists, match_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)





    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
