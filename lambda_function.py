import json
import boto3
import time
import csv
import datetime
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def lambda_handler(event, context):

    try:
        #Extract from S3
        
        bucket_name = 'curso-name'
        file_name = 'bank.csv'
        dir_file = '/tmp/' + file_name
        s3 = boto3.resource('s3')
        s3.meta.client.download_file(bucket_name, file_name, dir_file)

        # Transform Data      
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S" )
        line_count = 0
  
        connection = pymysql.connect(host='xxxx.xxxx.us-east-1.rds.amazonaws.com',
                        user='admin',
                        password='pass',
                        database='database')
        
        with open(dir_file, mode='r') as csv_file:
            data_csv = csv.reader(csv_file,delimiter=';')

            with connection:
                # Transform
                for row in data_csv:
                    line_count += 1
                    if line_count == 1:
                        pass
                    else:      
                        SQL_command = "INSERT INTO banktest.bank VALUES ("+row[0]+",'"+row[1]+"',"+row[5]+",'"+timestamp+"');"
                        with connection.cursor() as cursor:    
                            cursor.execute(SQL_command)
        
                        connection.commit()
            
        return {
            'statusCode': 200,
            'body': json.dumps('ETL Succesfully run')
        }

    except ValueError as e:
        print ("error main function")
        logger.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Server Error')
        }


