import numpy as np
import pandas as pd
import sqlite3 as sql

df = pd.read_csv("/home/gabriel/prog/data_samples/100Records.csv")


# Crear la Base de Datos
def createDB():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdbpan.db")
    conn.commit()
    conn.close()


# Limpiar los nombres de las columnas
def cleanColumns():
    df.columns = [x.upper().replace(" ","_").replace("-","_").replace("$","").replace("?","").replace("%","").replace(".","") \
                .replace("@","").replace("#","").replace(r"/","").replace("\\","").replace(r"(","").replace(")","") for x in df.columns]


# Insertar el csv a la Base de Datos
def insertData():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdbpan.db")
    df.to_sql(name='hundred', con=conn, if_exists="append", index=False)
    conn.close()


def showData():
    print(df)



#createDB()
#cleanColumns()
#insertData()
showData()
