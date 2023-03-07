import numpy as np
import pandas as pd
import sqlite3 as sql


covid_big = [
    "covid_big_1.csv", "covid_big_2.csv", "covid_big_3.csv", "covid_big_4.csv",
    "covid_big_5.csv", "covid_big_6.csv", "covid_big_7.csv", "covid_big_8.csv"
]


# Crear la Base de Datos
def createDB():
    conn=sql.connect("/home/gabriel/prog/csvSql/dbs/covidCases.db")
    conn.commit()
    conn.close()


# Insertar el csv a la Base de Datos
def insertData():

    for covid in covid_big:
        df = pd.read_csv('/home/gabriel/prog/csvSql/docs/' + covid)

        df.columns = [x.upper().replace(" ","_").replace("-","_").replace("$","").replace("?","").replace("%","").replace(".","") \
            .replace("@","").replace("#","").replace(r"/","").replace("\\","").replace(r"(","").replace(")","") for x in df.columns]

        conn=sql.connect("/home/gabriel/prog/csvSql/dbs/covidCases.db")
        df.to_sql(name="covid19", con=conn, if_exists="append", index=False)
        conn.close()


"""
createDB()
"""
insertData()
print("Full load")

