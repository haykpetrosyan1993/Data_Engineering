
import json
import awswrangler as wr
import psycopg2
import psycopg2.extras

def lambda_handler(event, context):


    # TODO

    countries = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/countries.csv', encoding = "ISO-8859-1")
    city = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/city.csv', encoding = "ISO-8859-1")
    captains = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/captains.csv', encoding = "ISO-8859-1")
    coaches = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/coaches.csv', encoding = "ISO-8859-1")
    home_away = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/home_away.csv', encoding = "ISO-8859-1")
    leagues = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/leagues.csv', encoding = "ISO-8859-1")
    main_sponsores = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/main_sponsores.csv', encoding = "ISO-8859-1")
    positions = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/positions.csv', encoding = "ISO-8859-1")
    results = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/results.csv', encoding = "ISO-8859-1")
    seasons = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/seasons.csv', encoding = "ISO-8859-1")
    shirt_sponsores = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/shirt_sponsores.csv', encoding = "ISO-8859-1")
    shotstype = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/shotstype.csv', encoding = "ISO-8859-1")
    situations = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/situations.csv', encoding = "ISO-8859-1")
    stadiums = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/stadiums.csv', encoding = "ISO-8859-1")
    teams = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/teams.csv', encoding = "ISO-8859-1")


    con = psycopg2.connect(host = "",
                        database = '',
                        user = "",
                        password = "",
                        port = '')

    cur = con.cursor()
    cur.execute("ROLLBACK")


    #---------------------------------------------------------------------country-------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS country
    (
    id INT PRIMARY KEY NOT NULL,
    country_name VARCHAR(200) NOT NULL,
    short_name VARCHAR(50) NOT NULL
     )
     """)
    lst = [tuple(i) for i in countries.values]
    try:
       query = """INSERT INTO country (id, country_name, short_name
       ) VALUES (%s,%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('country is done')

    #------------------------------------------------------------------city--------------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS city
    (
    id INT PRIMARY KEY NOT NULL,
    city_name VARCHAR(200) NOT NULL,
    country_id INT REFERENCES country(id)
     )
     """)
    lst = [tuple(i) for i in city.values]
    try:
       query = """INSERT INTO city (id, city_name, country_id
       ) VALUES (%s,%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('city is done')

    #-----------------------------------------------------------------------leagues-----------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS leagues
    (
    id INT PRIMARY KEY NOT NULL,
    league_name VARCHAR(200) NOT NULL,
    country_id INT REFERENCES country(id)
     )
     """)

    lst = [tuple(i) for i in leagues.values]
    try:
       query = """INSERT INTO leagues (id, league_name, country_id
       ) VALUES (%s,%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('leagues is done')
    # cur.execute("""DROP TABLE  IF EXISTS seasons""")


    #--------------------------------------------------------------------------------seasons-------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS seasons
    (
    id INT PRIMARY KEY NOT NULL,
    season INT NOT NULL
    )
      """)
    try:
       lst = [tuple(i) for i in seasons.itertuples(index=False)]
       query = """INSERT INTO seasons (id, season
       ) VALUES (%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('seasons is ')


    #--------------------------------------------------------------------------------shotstype--------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shotstype
    (
    id INT PRIMARY KEY NOT NULL,
    shotType VARCHAR(200) NOT NULL
     )
     """)

    lst = [tuple(i) for i in shotstype.values]
    try:
        query = """INSERT INTO shotstype (id, shotType
        ) VALUES (%s,%s);"""


        psycopg2.extras.execute_batch(cur, query, lst)
    except:
        print('shotstype is done')

    #-------------------------------------------------------------------------------results----------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS results
    (
    id INT PRIMARY KEY NOT NULL,
    result VARCHAR(200) NOT NULL
     )
     """)

    lst = [tuple(i) for i in results.values]
    try:
       query = """INSERT INTO results (id, result
       ) VALUES (%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('results is done')

    #----------------------------------------------------------------------------positions------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions
    (
    id INT PRIMARY KEY NOT NULL,
    positions_played VARCHAR(200) NOT NULL
     )
     """)

    lst = [tuple(i) for i in positions.values]
    try:
       query = """INSERT INTO positions (id, positions_played
       ) VALUES (%s,%s);"""


       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('positions is done')

    #------------------------------------------------------------------------shirt_sponsores----------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shirt_sponsores
    (
    id INT PRIMARY KEY NOT NULL,
    shirt_sponsore VARCHAR(200) NOT NULL
     )
     """)

    lst = [tuple(i) for i in shirt_sponsores.values]

    try:
       query = """INSERT INTO shirt_sponsores (id, shirt_sponsore
       ) VALUES (%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('shirt_sponsores is done')


    #--------------------------------------------------------------------------main_sponsores--------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS main_sponsores
    (
    id INT PRIMARY KEY NOT NULL,
    sponsore_title VARCHAR(200) NOT NULL
     )
     """)

    lst = [tuple(i) for i in main_sponsores.values]
    try:
       query = """INSERT INTO main_sponsores (id, sponsore_title
       ) VALUES (%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('main_sponsores is done')


    #------------------------------------------------------------------------stadiums----------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stadiums
    (
    id INT PRIMARY KEY NOT NULL,
    stadium_name VARCHAR(200) NOT NULL,
    city_id INT REFERENCES city(id) NOT NULL,
    capacity DECIMAL(20),
    country_id INT REFERENCES country(id)
     )
     """)

    lst = [tuple(i) for i in stadiums.values]
    try:
       query = """INSERT INTO stadiums (id, stadium_name, city_id, capacity, country_id
          ) VALUES (%s,%s, %s, %s, %s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('stadiums is done')





    #----------------------------------------------------------------------coaches--------------------------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS coaches(
             id INT PRIMARY KEY NOT NULL,
             coach_name VARCHAR(200),
             country_id INT REFERENCES country(id)
             )
             """)
    lst = [tuple(i) for i in coaches.values]

    try:
       query = """INSERT INTO coaches (id, coach_name, country_id
       ) VALUES (%s,%s, %s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('coaches is done')


    #--------------------------------------------------------------------teams-----------------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams
    (
    id INT PRIMARY KEY NOT NULL,
    team_title VARCHAR(200) NOT NULL,
    league_id INT REFERENCES leagues(id),
    country_id INT REFERENCES country(id),
    city_id INT REFERENCES city(id),
    main_sponsore_id INT REFERENCES main_sponsores(id),
    shirt_sponsore_id INT REFERENCES shirt_sponsores(id),
    coach_id INT REFERENCES coaches(id),
    stadium_id INT REFERENCES stadiums(id)
    captain_id INT REFERENCES players(id)
     )
     """)

    lst = [tuple(i) for i in teams.values]

    query = """INSERT INTO teams (id, team_title, league_id, country_id, city_id, main_sponsore_id, shirt_sponsore_id, captain_id, coach_id, stadium_id
       ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    psycopg2.extras.execute_batch(cur, query, lst)


    #---------------------------------------------------------------situations-------------------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS situations(
         id INT PRIMARY KEY NOT NULL,
         situation VARCHAR(200)
         )
         """)

    lst = [tuple(i) for i in situations.values]

    try:
       query = """INSERT INTO situations (id, situation
       ) VALUES (%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('situations is done')

    #--------------------------------------------------------------------home_away---------------------------------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_away(
         id INT PRIMARY KEY NOT NULL,
         home_away VARCHAR(200)
         )
         """)
    lst = [tuple(i) for i in home_away.values]
    try:
       query = """INSERT INTO home_away (id, home_away
       ) VALUES (%s,%s);"""

       psycopg2.extras.execute_batch(cur, query, lst)
    except:
       print('home_away is done')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
