#!/usr/bin/env python
# coding: utf-8

# # Pack

# In[1]:


import os
import time
import datetime
import pyspark.sql.functions as f
from uuid import *
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from uuid import UUID
import time_uuid 
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W


# In[2]:


import findspark
findspark.init()


# In[3]:


spark = SparkSession.builder.config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.2.0')\
							.config('spark.network.timeout','36000s')\
							.config('spark.driver.memory','16g')\
							.config('spark.executor.memory','1g')\
							.config('spark.driver.maxResultSize','16g')\
							.config('spark.executor.heartbeatInterval','18000s')\
							.getOrCreate()


# In[4]:


spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


# In[5]:


def process_data(df):
    spark_time = df.select('create_time').collect()
    normal_time = []
    for i in range(len(spark_time)):
        a = time_uuid.TimeUUID(bytes=UUID(df.select('create_time').collect()[i][0]).bytes).get_datetime().strftime(('%Y-%m-%d %H:%M:%S'))
        normal_time.append(a)
    spark_timeuuid = []
    for i in range(len(spark_time)):
        spark_timeuuid.append(spark_time[i][0])
    time_data = spark.createDataFrame(zip(spark_timeuuid,normal_time),['create_time','ts'])
    result = df.join(time_data,['create_time'],'inner').drop(df.ts)
    result = result.select('create_time','ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    return result


# In[6]:


def calculating_clicks(df):
    click = df.filter(df.custom_track == 'click')
    click = click.na.fill({'bid':0})
    click = click.na.fill({'job_id':0})
    click = click.na.fill({'campaign_id':0})
    click = click.na.fill({'group_id':0})
    click = click.na.fill({'publisher_id':0})
    click.createOrReplaceTempView('clicks')
    click_output = spark.sql("""select job_id, ts as date, hour(ts) as hour, publisher_id, campaign_id, group_id, avg(bid) as bid_set,count(*) as click, sum(bid) as spend_hour 
                                from clicks
                                group by job_id, ts, hour(ts), publisher_id, campaign_id, group_id""")
    return click_output


# In[7]:


def calculating_conversion(df):
    conversion = df.filter(df.custom_track == 'conversion')
    conversion = conversion.na.fill({'job_id':0})
    conversion = conversion.na.fill({'campaign_id':0})
    conversion = conversion.na.fill({'group_id':0})
    conversion = conversion.na.fill({'publisher_id':0})
    conversion.createOrReplaceTempView('conversion')
    conversion_output = spark.sql("""select job_id, ts as date, hour(ts) as hour, publisher_id, campaign_id, group_id, count(*) as conversion
                                    from conversion
                                    group by job_id, ts, hour(ts), publisher_id, campaign_id, group_id""")
    return conversion_output


# In[8]:


def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createOrReplaceTempView('qualified')
    qualified_output = spark.sql("""select job_id , ts as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as qualified  
                                    from qualified
                                    group by job_id , ts , hour(ts) , publisher_id , campaign_id , group_id """)
    return qualified_output


# In[9]:


def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createOrReplaceTempView('unqualified')
    unqualified_output = spark.sql("""select job_id , ts as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified  
                                    from unqualified
                                    group by job_id , ts , hour(ts) , publisher_id , campaign_id , group_id """)
    return unqualified_output


# In[10]:


def process_final_data(click_output,conversion_output,qualified_output,unqualified_output):
    final_data = click_output.join(conversion_output,['job_id', 'date', 'hour',  'publisher_id', 'campaign_id', 'group_id'],'full')\
                .join(qualified_output,['job_id', 'date', 'hour',  'publisher_id', 'campaign_id', 'group_id'],'full')\
                .join(unqualified_output,['job_id', 'date', 'hour',  'publisher_id', 'campaign_id', 'group_id'],'full')
    return final_data


# In[11]:


def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data


# In[12]:


def get_company_data(url,driver,user,password):
    sql = """(select id as job_id, company_id FROM job) as company"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company


# In[13]:


def import_to_mysql(output):
    output = output.withColumn('last_update_times',lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))).withColumn('sources',lit('Cassandra'))
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversion','click','bid_set','spend_hour','last_update_times','sources')
    final_output.write.format('jdbc')\
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3310/de_learning") \
    .option("dbtable", "events_etl") \
    .mode("append") \
    .option("user", "root") \
    .option("password", "sa") \
    .save()
    print('+--------------------------------------------+')
    return print('| Data imported successfully                 |')


# In[14]:


def get_latest_time_cassandra():
    cassandra_time = spark.read.format('org.apache.spark.sql.cassandra').options(table = 'tracking', keyspace = 'de_learning').load().agg({'ts':'max'}).take(1)[0][0]
    return cassandra_time


# In[15]:


def get_latest_time_mysql(url, driver, user, password):
    sql = """(select max(last_update_times) from events_etl) as mysqltime"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load().take(1)[0][0]
    if mysql_time is None:
        lated_time = '1999-12-31 23:59:59'
    else :
        lated_time = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return lated_time


# In[22]:


def main_task(mysql_time):
    driver = 'com.mysql.cj.jdbc.Driver'
    url = 'jdbc:mysql://localhost:3310/de_learning'
    user = 'root'
    password = 'sa'
    print('+--------------------------------------------+')
    print("| Retrieve data from Cassandra               |")
    df = spark.read.format('org.apache.spark.sql.cassandra').options(table = 'tracking', keyspace = 'de_learning').load().where(col('ts') >= mysql_time)
    print('+--------------------------------------------+')
    print("| Process data...                            |")
    data = process_data(df)
    cassandra_output = process_cassandra_data(data)
    print('+--------------------------------------------+')
    print("| Retrieve Company_ID                        |")
    company_data = get_company_data(url,driver,user,password)
    print('+--------------------------------------------+')
    print("| Finalizing Output                          |")
    final_output = cassandra_output.join(company_data,'job_id','left')
    import_to_mysql(final_output)
    print('+--------------------------------------------+')
    return print('| Task Completed!                            |')


# In[23]:


while True:
    print('+--------------------------------------------+')
    print('| Building Data Pipeline Task                |')
    print('+--------------------------------------------+')
    driver = 'com.mysql.cj.jdbc.Driver'
    url = 'jdbc:mysql://localhost:3310/de_learning'
    user = 'root'
    password = 'sa'
    timestart = datetime.datetime.now()
    print('+--------------------------------------------+')
    print('| Time start at {}   |'.format(timestart))
    last_cassandra_time = get_latest_time_cassandra()
    last_mysql_time = get_latest_time_mysql(url, driver, user, password)
    if last_cassandra_time > last_mysql_time:
        main_task(last_mysql_time)
    else:
        print('+--------------------------------------------+')
        print('| New Data not found!                        |')
    timeend = datetime.datetime.now()
    print('+--------------------------------------------+')
    print('| Time end at {}     |'.format(timeend))
    print('+--------------------------------------------+')
    print('| Job take {} seconds to execute! '.format((timeend - timestart).total_seconds()))
    print('+--------------------------------------------+')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    time.sleep(30)


# In[20]:


last_cassandra_time


# In[21]:


last_mysql_time

