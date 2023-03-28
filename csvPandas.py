import numpy as np
import pandas as pd
import sqlite3 as sql


covid_big = [
    "covid_big_1.csv", "covid_big_2.csv", "covid_big_3.csv", "covid_big_4.csv",
    "covid_big_5.csv", "covid_big_6.csv", "covid_big_7.csv", "covid_big_8.csv"
]


# DIVIDE BIG CSV FILES
def splitCsv():
    chunk_size = 900000
    batchNum = 1

    for chunk in pd.read_csv("/home/gabriel/prog/csvSql/docs/Casos_positivos_de_COVID-19_en_Colombia.csv", chunksize=chunk_size):
        chunk.to_csv("/home/gabriel/prog/csvSql/docs/covid_big_" + str(batchNum) + ".csv", index=False)
        batchNum += 1


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
            .replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","Ú")
            .replace("@","").replace("#","").replace(r"/","").replace("\\","").replace(r"(","").replace(")","") for x in df.columns]

        conn=sql.connect("/home/gabriel/prog/csvSql/dbs/covidCases.db")
        df.to_sql(name="covid19", con=conn, if_exists="append", index=False)
        conn.close()


# Filtrar data
# fields === Emp_ID, Name_Prefix, First_Name, Middle_Initial, Last_Name, Gender, Email
def search():
    conn=sql.connect("/home/gabriel/prog/csvSql/dbs/covidCases.db")
    cursor=conn.cursor()LECT COUNT(ID_DE_CASO) FROM covid19 WHERE NOMBRE_MUNICIPIO 
    sqlQuery = f"""SELECT COUNT(ID_DE_CASO) FROM covid19 WHERE NOMBRE_MUNICIPIO = 'CARTAGENA'"""
    cursor.execute(sqlQuery)
    datos = cursor.fetchall()
    conn.commit()
    conn.close()
    for dato in datos:
        print(dato)


"""
splitCsv()

createDB()

insertData()
print("Full load")
"""
search()
