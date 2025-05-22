import json
import boto3
import csv
import datetime
import logging
import pymysql
import os
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables de entorno seguras
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
TABLE_NAME = 'bank'
BUCKET_NAME = 'curso-name'
FILE_NAME = 'bank.csv'
BATCH_SIZE = 50

def lambda_handler(event, context):
    try:
        # Descargar archivo desde S3
        local_path = f'/tmp/{FILE_NAME}'
        s3 = boto3.resource('s3')
        s3.meta.client.download_file(BUCKET_NAME, FILE_NAME, local_path)
        logger.info("Archivo CSV descargado exitosamente.")

        rows = parse_csv(local_path)
        insert_into_aurora(rows)

        return {
            'statusCode': 200,
            'body': json.dumps(f'ETL finalizada con {len(rows)} filas insertadas.')
        }

    except Exception as e:
        logger.error("Error general:\n%s", traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps('Error en el servidor: ' + str(e))
        }

def parse_csv(file_path):
    rows = []
    with open(file_path, mode='r') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        next(reader, None)  # saltar encabezado

        for row in reader:
            if len(row) != 17:
                logger.warning("Fila inválida (esperado 17 columnas): %s", row)
                continue
            try:
                # Conversión segura
                parsed = (
                    int(row[0]),                                 # age
                    row[1], row[2], row[3], row[4],              # job, marital, education, default
                    int(row[5]),                                 # balance
                    row[6], row[7], row[8],                      # housing, loan, contact
                    int(row[9]),                                 # day
                    row[10],                                     # month
                    int(row[11]), int(row[12]), int(row[13]),    # duration, campaign, pdays
                    int(row[14]),                                # previous
                    row[15], row[16]                             # poutcome, y
                )
                rows.append(parsed)
            except Exception as e:
                logger.warning("Error parseando fila: %s", row)

    logger.info("Total de filas válidas parseadas: %d", len(rows))
    return rows

def insert_into_aurora(data):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=False
    )

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (
            age, job, marital, education, `default`, balance, housing, loan, contact,
            day, month, duration, campaign, pdays, previous, poutcome, y
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    with connection:
        with connection.cursor() as cursor:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i + BATCH_SIZE]
                cursor.executemany(insert_query, batch)
                connection.commit()
                logger.info("Batch insertado: filas %d a %d", i + 1, i + len(batch)
