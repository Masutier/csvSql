import csv
import sqlite3 as sql
import pandas as pd



# DIVIDE BIG CSV FILES
def splitCsv():
    chunk_size = 900000
    batchNum = 1

    for chunk in pd.read_csv("/home/gabriel/prog/csvSql/docs/Casos_positivos_de_COVID-19_en_Colombia.csv", chunksize=chunk_size):
        chunk.to_csv("/home/gabriel/prog/csvSql/docs/covid_big_" + str(batchNum) + ".csv", index=False)
        batchNum += 1


# Crea la db
def createDB():
    conn=sql.connect('/home/gabriel/prog/csvSql/dbs/covidCases.db')
    conn.commit()
    conn.close()


def csv_sql(file_dir, table_name, database_name):
    conn = sql.connect(database_name)
    cur = conn.cursor()
    # Drop the current table by: 
    cur.execute("DROP TABLE IF EXISTS %s;" % table_name)

    with open(file_dir, 'r') as fl:
        hd = fl.readline()[:-1].split(',')
        ro = fl.readlines()
        db = [tuple(ro[i][:-1].split(',')) for i in range(len(ro))]

    header = ','.join(hd)
    cur.execute("CREATE TABLE IF NOT EXISTS %s (%s);" % (table_name, header))
    cur.executemany("INSERT INTO %s (%s) VALUES (%s);" % (table_name, header,('?,' * len(hd))[:-1]), db)
    conn.commit()
    conn.close()



#splitCsv()
#createDB()
#csv_sql('/home/gabriel/prog/csvSql/docs/Casos_positivos_de_COVID-19_en_Colombia.csv', 'covid', '/home/gabriel/prog/csvSql/dbs/covidCases.db')
covid_big = [
    "/covid_big_1.csv", "/covid_big_2.csv", "/covid_big_3.csv", "/covid_big_4.csv", "/covid_big_5.csv",
    "/covid_big_6.csv", "/covid_big_7.csv", "/covid_big_8.csv"
    ]

for covid in covid_big:
    print(covid)
    csv_sql('/home/gabriel/prog/csvSql/docs/covid_big_1.csv', 'covid', '/home/gabriel/prog/csvSql/dbs/covidCases.db')

print("Full load")
