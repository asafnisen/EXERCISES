"""
    this exercises include :
        get data from CSV by pandas
        get data from API request
        groupby pandas + countif + agg
        insert to sql server whith pandas using 2 types of connection (only table 2)
        at the end read from sql by pandas sql to dataframe
"""

import requests
import base64
import json
import pandas as pd

import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine

server = 'LTH80051170\SQLEXPRESS'
database = 'TEST_asaf'
engine = create_engine('mssql+pyodbc://' + server + '/' + database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

"""
# pyodbc
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LTH80051170\SQLEXPRESS;'
                      'Database=TEST_asaf;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT * FROM asaf')

#for i in cursor:
#    print(i)
"""

 # A!123456
 #asafnisen

def check_domain(domain):

    #domain = "fourthgate.org/Yryzvt"
    url_id = base64.urlsafe_b64encode(domain.encode()).decode().strip("=")
    url = "https://www.virustotal.com/api/v3/urls/" + url_id
    headers = {"x-apikey": "f79fd3be4b9ff488baa9d00b453fa7c5642d119c9d0ae4a718e6dc9dddf3f887"}
    response = requests.get(url, headers=headers)
    res=(response.json())

    # print(response.text)
    #p =(json.dumps(res,indent=1))
    #y = json.loads(p)


    ######################
    # Table 1 Risk or safe
    ######################
    #opp1
    """
    df= pd.DataFrame.from_dict(res["data"]["attributes"]["last_analysis_results"] ,orient='index')
    df['domain'] = domain
    df2=(df.groupby('domain')["result"].apply(lambda x: 1 * ((df.result == 'malicious') | (df.result == 'phishing') | (df.result == 'malware')).sum()))   # ellif EXAMPLE
    df2= pd.DataFrame(df2)
    df2["status"] = df2["result"].apply(lambda x: "risk" if x >0 else "safe")
    print (df2["status"])
    """

    #opp2
    """
    df= pd.DataFrame.from_dict(res["data"]["attributes"]["last_analysis_results"] ,orient='index')    
    a = (  (df[(df.result == 'malicious') | (df.result == 'phishing') | (df.result == 'malware')]['result'].count()) )
    L = [( "risk" if a >0 else "safe"), domain]
    print (L)
    """

    #opp3
    """
    df= pd.DataFrame.from_dict(res["data"]["attributes"]["last_analysis_results"] ,orient='index')
    df['domain'] = domain
    df2=(df.groupby('domain').agg({"result": lambda x: 1 * ((df.result == 'malicious') | (df.result == 'phishing') | (df.result == 'malware')).sum() }))
    
    data = pd.DataFrame({
        'domain': df2.index[0],
        'result': df2['result'].values
    })
    print  (data)
    """

    ######################
    # Table 2
    ######################
    """
    df= pd.DataFrame.from_dict(res["data"]["attributes"]["last_analysis_results"] ,orient='index')
    df3=(df.groupby('result')["result"].count().reset_index(name="count"))
    df3['domain'] = domain   # T2

    #to SQL SERVER 2 methodes :

    df3.to_sql('domain_result_count', con=engine, if_exists='append', chunksize=1000,index=True) # insert index column. columns name must be same like SQL
    #df3.to_sql('domain_result_count', con=engine, if_exists='append', chunksize=1000,index=False) # insert sql no index column
    #for index, row in df3.iterrows():
    #    engine.execute("INSERT INTO domain_result_count([index],[result],[count],[domain]) values (?,?,?,?)", index,
    #                   row['result'], row['count'], row['domain'])

    """
#main

#colnames=['A']
#csv=pd.read_csv(r"C:\PYTHON\request1.csv",header=None,names=colnames)

#for row in csv.index:
    #domain=(csv['A'][row])
    #check_domain(domain)

domain = "fourthgate.org/Yryzvt"
df = check_domain(domain)

print (df)

df = pd.read_sql_query('SELECT * FROM domain_result_count', engine)

#print (df)
