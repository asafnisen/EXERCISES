import requests
import json
import pandas
import pandas as pd
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine

"""
    this exercises include :
        get data from API request imdb
        insert to sql server whith pandas - change index
        concat tadaframes
"""

server = 'LTH80051170\SQLEXPRESS'
database = 'TEST_asaf'
engine = create_engine('mssql+pyodbc://' + server + '/' + database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

# table1 -  movies
parameters_Top250Movies = {"apiKey": "k_4ha29bz5"}
response = requests.get("https://imdb-api.com/en/API/Top250Movies/", params=parameters_Top250Movies)
# or
#response = requests.get("https://imdb-api.com/en/API/Top250Movies/k_4ha29bz5")
res=(response.json())
df= pd.DataFrame(res['items'])
df2= df[['id','rank','imDbRating','title','year']]
df2.set_index("id",inplace=True)
df2.to_sql('imdb_movies', con=engine, if_exists='append', chunksize=1000,index=True)


# table2 -  FullCast

parameters_FullCast = {"apiKey": "k_4ha29bz5", "id": "tt0111161"}

response = requests.get("https://imdb-api.com/en/API/FullCast/", params=parameters_FullCast)
#response = requests.get("https://imdb-api.com/en/API/FullCast/k_f3mxqolc/tt0111161")
res= (response.json())
#df2 = pd.DataFrame(res["imDbId"])
#print(json.dumps(res,indent=1))
#df= pd.DataFrame.from_dict(res)
print (res['imDbId'])
#res.get(key) for key in res
print (type(res))

required_fields = ['imDbId', 'directors', 'writers', 'actors']
new_dict = {key:value for key, value in res.items() if key in required_fields}  # new dict only columns needed. dict Comprehension
#new_list = [value  for key, value in res.items() if key in required_fields]  # LIST Comprehension

#directors
df= pd.DataFrame(new_dict['directors']['items'])
df['job'] = (new_dict['directors']['job'])
df['imDbId'] = (new_dict['imDbId'])
df1= df[['id','name','job','imDbId']]

#writers
df= pd.DataFrame(new_dict['writers']['items'])
df['job'] = (new_dict['directors']['job'])
df['imDbId'] = (new_dict['imDbId'])
df2= df[['id','name','job','imDbId']]

#actors
df= pd.DataFrame(new_dict['actors'])
df['job'] = 'actors'
df['imDbId'] = (new_dict['imDbId'])
df3= df[['id','name','job','imDbId']]

df4 = pd.concat([df1, df2,df3])
#df4.to_sql('imdb_cast', con=engine, if_exists='append', chunksize=1000,index=False)

print (df4)


for index,row in df4.iterrows():
    #L = (str(row['id']), str(row['name']),str( row['job']), str(row['imDbId']))
    #L=list(L)
    sql = ('INSERT INTO dbo.imdb_cast([id],[name],[job],[imDbId]) values (%s,%s,%s,%s)' %(row['id'], row['name'], row['job'], row['imDbId']))
    #id_1=row['id']
    #name_1=row['name']
    #job_1 = row['job']
    #imDbId_1 = row['imDbId']
    #sql = ('INSERT INTO dbo.imdb_cast([id],[name],[job],[imDbId]) values (%s,%s,%s,%s)' %(id_1,name_1,job_1,imDbId_1))
    #engine.execute(sql)
    break
