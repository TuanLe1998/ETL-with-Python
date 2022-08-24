import os
import sys
import pandas as pd
import psycopg2
import configparser
import requests
import datetime
import json
import decimal


# get data from configuration file
config = configparser.ConfigParser()
try:
    config.read('config.ini')
except Exception as e:
    print('could not read configuration file:' + str(e))
    sys.exit()


# read settings from configuration file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('could not make request:' + str(e))
    sys.exit()

# read settings from configuration file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# initialize list of lists for data storage
BOCDates = []
BOCRates = []

# check response status and process BOC JSON object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
    # Append date and rate to 2 empty lists defined above
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row["FXUSDCAD"]['v']))

# Create a zip to store data of each days
BOCdata = list(zip(BOCDates, BOCRates))

# Append to table
try:
    connection = psycopg2.connect(user="abc",
                                  password="abc",
                                  host="abc",
                                  port="abc",
                                  database="acc")
    cursor = connection.cursor()
    for row in BOCdata:
        postgres_insert_query = """ INSERT INTO expenses (day, rate) VALUES (%s,%s)"""
        record_to_insert = row
        cursor.execute(postgres_insert_query, record_to_insert)

    connection.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into expenses table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into expenses table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
