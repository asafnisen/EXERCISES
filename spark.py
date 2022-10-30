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

df1= df1.withColumnRenamed("Ad Format","Ad_Format").withColumn("Advertiser_Name", lit("Rubicon"))

df1 = df1.where ("Ad_Format = 'Video'")

newcolumns = ['Date','Advertiser_Name','Domain','Revenue']
df_Rubicon = df1.select("Date","Advertiser_Name","Referring Domain","Publisher Net Revenue").toDF(*newcolumns) # ordering the data

## GROUP DATE 



df_Rubicon= (df_Rubicon.withColumn("Revenue",df_Rubicon.Revenue.cast("float")))  # לא חובה
all_video_advertisers= (all_video_advertisers.withColumn("Revenue",all_video_advertisers.Revenue.cast("float")))    

all_video_advertisers =  all_video_advertisers.groupBy("Date","Advertiser_Name","Domain").sum("Revenue")
df_Rubicon =  df_Rubicon.groupBy("Date","Advertiser_Name","Domain").sum("Revenue")


# print (df_Rubicon.groupBy("Date","Advertiser_Name","Domain").count().show())   דוגמה 
# עוד דוגמה 
# פייטון מתבלבל בין פקודה SUM של SPARK לבין פנימית
# לכן צריך לציין לו 
#import pyspark.sql.functions as psf
#print (df_Rubicon.groupBy("Date","Advertiser_Name","Domain").agg(psf.sum("Revenue")).show())



#print (df_Rubicon.show())
#print (all_video_advertisers.show())

df_Rubicon.createOrReplaceTempView("temp")
all_video_advertisers.createOrReplaceTempView("temp2")

SQL = " select ROW_NUMBER() OVER(PARTITION BY Advertiser_Name,Domain ORDER BY Date ASC,order_a) ,* from (SELECT *, 1 as {} FROM temp union SELECT * , 2  FROM temp2) t where Advertiser_Name = 'Rubicon' order by Advertiser_Name,Date,Domain ".format("order_a")
sqlDF = spark.sql(SQL)

print (SQL)
print ("##################################################################################")
print ("##################################################################################")
sqlDF.show()





       