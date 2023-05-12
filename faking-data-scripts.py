#!/usr/bin/env python
# coding: utf-8
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import  Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from datetime import datetime, timedelta
import time
import cassandra
import random
import uuid
import math
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import mysql.connector

import warnings
warnings.filterwarnings('ignore')

#connect to mysql
host = 'localhost'
port = 3310
database = 'de_learning'
user = 'root'
password = 'sa'

#connect to cassandra
keyspace = 'de_learning'
cluster = Cluster()
session = cluster.connect(keyspace)

def get_data_from_job(user,password,host,port,database):
    cnx = mysql.connector.connect(user = user, password = password, host = host, port = port, database = database)
    query = """select id as job_id, campaign_id, group_id, company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher(user,password,host,port,database):
    cnx = mysql.connector.connect(user = user, password = password, host = host, port = port, database = database)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def tracking_generating(n_records,session,user,password,host,db_name):
    print('| Connecting to server...                    |')
    print('+--------------------------------------------+')
    publisher = get_data_from_publisher(user,password,host,port,database)
    publisher_list = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job(user,password,host,port,database)
    jobs_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    i = 0
    record = n_records
    while i <= record:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        campaign_id = random.choice(campaign_list)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        group_id = random.choice(group_list)
        job_id = random.choice(jobs_list)
        publisher_id = random.choice(publisher_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """insert into tracking(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) values('{}',{},{},'{}',{},{},{},'{}')""".format(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts)
        session.execute(sql)
        i+=1
    return print("| Data Generated Successfully                |")

status = 'ON'
while status == 'ON':
    print('+--------------------------------------------+')
    print('| Fake new data into Datalake (Cassandra DB) |')
    print('+--------------------------------------------+')
    n_records = random.randint(1,9)
    print('+--------------------------------------------+')
    print('| Insert {} records to DataLake               |'.format(n_records + 1))
    print('+--------------------------------------------+')
    tracking_generating(n_records,session,user,password,host,database)
    print('+--------------------------------------------+')
    print('| Task Completed!                            |')
    print('+--------------------------------------------+')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    time.sleep(10)
