import pyodbc
import pandas as pd
import sqlalchemy
from config import server
from config import database
from config import username
from config import password

def extractData():
    try:
        # print(pyodbc.drivers())
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=no;UID='+username+';PWD='+ password,Trusted_Connection='Yes')
        cursor = cnxn.cursor()
        # # Execute a query
        # cursor.execute('SELECT * FROM DimAccount')
        #this query will get all tables
        alltablequery='SELECT name FROM AdventureWorksDW2019.sys.tables'
        cursor.execute(alltablequery)
        tables=cursor.fetchall()
        # print(tables)
        #f'' is string literal. it replaces {} this with value e.g.in below example it will replace {i[0]} with tablename
        for i in tables:
            df=pd.read_sql_query(f'SELECT * FROM {i[0]}',cnxn)
            loadDataToPostgres(df,i[0])
        # # Fetch the results
    except Exception as e:
        print("Data Extraction Error.."+str(e))
    finally:
        cnxn.close()

def loadDataToPostgres(df,tbl):
    engine = sqlalchemy.create_engine('postgresql://etl:demopass@localhost:5432/Adventureworks')
    # print(tbl)
    df.to_sql(tbl, engine, index=False, if_exists='replace')
    print("data imported successfully for the table {} with rows count {}".format(tbl,len(df)))

extractData()