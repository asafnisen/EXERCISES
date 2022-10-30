import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import pyodbc
from sql_config import SQL_SERVER_server, SQL_SERVER_database
import sqlalchemy as sal
from sqlalchemy import create_engine

conn = pyodbc.connect('Driver={SQL Server};'+
                      'Server={};'.format(SQL_SERVER_server)+
                      'Database={};'.format(SQL_SERVER_database)+
                      'Trusted_Connection=yes;')

engine = create_engine('mssql+pyodbc://' + SQL_SERVER_server + '/' + SQL_SERVER_database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

## all_video_advertisers 
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.read.option('header','true').csv("C:\PYTHON\\all_video_advertisers.CSV")
df2= df.where("not (_c1 is null and _c2 is null and _c3 is null )") 
newcolumns = ['Date','Advertiser_Name','Domain','Revenue']
df2= df2.toDF(*newcolumns)
# FILTERS 
#3 אופציות
#all_video_advertisers = df2.where(~df2.Date.contains('Date')) 
#או 
# df2.where(df2['Date'] !='Date').show()
#או 

all_video_advertisers = df2.where("Date != 'Date'")         # ordering the data



## Rubicon  
df1= spark.read.option("multiline","true") \
      .json(r"C:\PYTHON\Rubicon.json")

df1= df1.withColumn("Advertiser_Name", lit("Rubicon"))

newcolumns = ['Date','Advertiser_Name','Domain','Revenue']
df_Rubicon = df1.select("Date","Advertiser_Name","Referring Domain","Publisher Net Revenue").toDF(*newcolumns) # ordering the data


df_Rubicon.createOrReplaceTempView("temp")
all_video_advertisers.createOrReplaceTempView("temp2")
sqlDF = spark.sql(" select ROW_NUMBER() OVER(PARTITION BY Advertiser_Name,Domain ORDER BY Date ASC) ,* from (SELECT * FROM temp union SELECT * FROM temp2) t where Advertiser_Name = 'Rubicon' order by Advertiser_Name,Domain ")


## דוגמה שליפה ODBC + ALCHEMY

cursor = conn.cursor()
cursor.execute('SELECT * FROM asaf')

for i in cursor:
    print(i)


