import json
import awswrangler as wr
import psycopg2
import psycopg2.extras
import boto3
import csv


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.resource('s3')
    #-------------------------------------------------------------matches data ---------------------------------------------------------------------
    bucket_main = s3.Bucket('football-data-buckets')
    key = 's3-bucket/match_details_2021.csv'
    local_file_name = '/tmp/match_details_2021.csv'
    s3.Bucket('football-data-buckets').download_file(key,local_file_name)
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/match_details_trans.csv'
    local_file_trans = '/tmp/match_details_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)


    # write the data into '/tmp' folder
    with open('/tmp/match_details_trans.csv','r') as infile:
        lst = list(csv.reader(infile))


    # write the data into '/tmp' folder
    with open('/tmp/match_details_2021.csv','r') as infile:
         reader = list(csv.reader(infile))
         reader = reader[::-1] # the date is ascending order in file
         for i in lst:
             if i not in reader:
                    reader.insert(0, i)

    with open('/tmp/match_details_2021.csv', 'w', newline='') as outfile:
         writer = csv.writer(outfile)
         for line in reversed(reader): # reverse order
             writer.writerow(line)

      # upload file from tmp to s3 key
    bucket_main.upload_file('/tmp/match_details_2021.csv', key)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
