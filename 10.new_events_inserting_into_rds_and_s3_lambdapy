import json
import awswrangler as wr
import psycopg2
import psycopg2.extras
import boto3
import csv

def lambda_handler(event, context):

    s3 = boto3.resource('s3')
    # TODO implement
    # players_trans = None
    # # matches_trans = None
    # # match_details_trans = None
    # # player_details_trans = None


    players_trans = wr.s3.read_csv(path='s3://football-data-new-actions/s3-bucket-trans/players_trans.csv', encoding = "ISO-8859-1")



    matches_trans = wr.s3.read_csv(path='s3://football-data-new-actions/s3-bucket-trans/matches_trans.csv', encoding = "ISO-8859-1")


    match_details_trans = wr.s3.read_csv(path='s3://football-data-new-actions/s3-bucket-trans/match_details_trans.csv', encoding = "ISO-8859-1")


    player_details_trans = wr.s3.read_csv(path='s3://football-data-new-actions/s3-bucket-trans/player_details_trans.csv', encoding = "ISO-8859-1")

    con = psycopg2.connect(host = "",
                         database = "",
                         user = "",
                         password = "",
                         port = '')
    cur = con.cursor()
    cur.execute("ROLLBACK")
     # -------------------------------------------------------------------players----------------------------------------------------------------------------

    lst = [tuple(i) for i in players_trans.itertuples(index=False)]

    query = """INSERT INTO players (id, player_name, country_id
              ) VALUES (%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)




    # ------------------------------------------------------------------matches---------------------------------------------------------------------------------

    lst = [tuple(i) for i in matches_trans.values]

    query = """INSERT INTO matches (id, match_date, home_team_id, guest_team_id, home_team_goals, guest_team_goals
       ) VALUES (%s,%s,%s,%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)


    # -----------------------------------------------------------------match_details---------------------------------------------------------------------------
    lst = [tuple(i) for i in match_details_trans.itertuples(index=False)]

    query = """INSERT INTO match_details (id, match_id, player_id, h_team_id, a_team_id, minute, result_id, season_id, shotType_id, h_a, situation_id
       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)

     # -----------------------------------------------------------------players_results-------------------------------------------------------------------------
    lst = [tuple(i) for i in player_details_trans.itertuples(index=False)]

    query = """INSERT INTO players_results (id, goals, own_goals, shots, time, player_id, team_id, position_id, h_a, yellow_card, red_card, key_passes,
         assists, match_id
         ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""


    psycopg2.extras.execute_batch(cur, query, lst)


    #------------------------------------------------------------------deleting matches rows--------------------------------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/matches_trans.csv'
    local_file_trans = '/tmp/matches_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)


    # write the data into '/tmp' folder
    with open('/tmp/matches_trans.csv','r') as infile:
        reader = list(csv.reader(infile))
        del reader[1:]
        # reader = reader[::-1] # the date is ascending order in file

    with open('/tmp/matches_trans.csv', 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reader: # reverse order
            writer.writerow(line)

    # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/matches_trans.csv', key_trans)

    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/players_trans.csv'
    local_file_trans = '/tmp/players_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)

    # --------------------------------------------------------------------------deleting players rows------------------------------------------------------------------------------------
    # write the data into '/tmp' folder
    with open('/tmp/players_trans.csv','r', encoding = "ISO-8859-1") as infile:
        reader = list(csv.reader(infile))
        del reader[1:]
        # reader = reader[::-1] # the date is ascending order in file

    with open('/tmp/players_trans.csv', 'w', encoding = "ISO-8859-1", newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reader: # reverse order
            writer.writerow(line)

    # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/players_trans.csv', key_trans)

    # --------------------------------------------------------------------------deleting match_details rows-------------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/match_details_trans.csv'
    local_file_trans = '/tmp/match_details_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)


    # write the data into '/tmp' folder
    with open('/tmp/match_details_trans.csv','r') as infile:
        reader = list(csv.reader(infile))
        del reader[1:]
        # reader = reader[::-1] # the date is ascending order in file


    with open('/tmp/match_details_trans.csv', 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reader: # reverse order
            writer.writerow(line)

    # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/match_details_trans.csv', key_trans)

    # -----------------------------------------------------------------------deleting player_details_rows----------------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/player_details_trans.csv'
    local_file_trans = '/tmp/player_details_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)


    # write the data into '/tmp' folder
    with open('/tmp/player_details_trans.csv','r') as infile:
        reader = list(csv.reader(infile))
        del reader[1:]
        # reader = reader[::-1] # the date is ascending order in file


    with open('/tmp/player_details_trans.csv', 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reader: # reverse order
            writer.writerow(line)

    # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/player_details_trans.csv', key_trans)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
