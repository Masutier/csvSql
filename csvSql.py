import csv
import sqlite3 as sql


# Lee el csv
def readCsv():
    csvData = []
    with open("/home/gabriel/prog/data_samples/100Records.csv", newline = '') as csvfile:
        csvRowData = csv.reader(csvfile, delimiter = ',')
        for row in csvRowData:
            csvData.append(row)
    return csvData


# Crea la db
def createDB():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    conn.commit()
    conn.close()


# Crea la tabla en la db
def createTable():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    cursor.execute(
        """CREATE TABLE hundred (
            Emp_ID INTEGER,
            Name_Prefix VARCHAR,
            First_Name VARCHAR,
            Middle_Initial VARCHAR,
            Last_Name VARCHAR,
            Gender VARCHAR,
            Email VARCHAR
        )"""
    )
    conn.commit()
    conn.close()


# crea una fila en la db
def insertRowData(Emp_ID,Name_Prefix,First_Name,Middle_Initial,Last_Name,Gender,Email):
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    sqlQuery = f"""INSERT INTO hundred VALUES (
        {Emp_ID},
        '{Name_Prefix}',
        '{First_Name}',
        '{Middle_Initial}',
        '{Last_Name}',
        '{Gender}',
        '{Email}'
        )"""
    cursor.execute(sqlQuery)
    conn.commit()
    conn.close()


# carga el archivo csv en la db
def insertAllData():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    csvData = readCsv()

    for row in csvData:
        Emp_ID = row[0]
        Name_Prefix = row[1]
        First_Name = row[2]
        Middle_Initial = row[3]
        Last_Name = row[4]
        Gender = row[5]
        Email = row[6]

        sqlQuery = f"""INSERT INTO hundred VALUES (
            '{Emp_ID}',
            '{Name_Prefix}',
            '{First_Name}',
            '{Middle_Initial}',
            '{Last_Name}',
            '{Gender}',
            '{Email}'
            )"""
        cursor.execute(sqlQuery)
    conn.commit()
    conn.close()


# Leer db y ordenar data
# fields === Emp_ID, Name_Prefix, First_Name, Middle_Initial, Last_Name, Gender, Email
def readOrdered(field):
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    sqlQuery = f"""SELECT * FROM hundred ORDER BY {field}"""
    cursor.execute(sqlQuery)
    datos = cursor.fetchall()
    conn.commit()
    conn.close()
    for dato in datos:
        print(dato)


# Filtrar data
# fields === Emp_ID, Name_Prefix, First_Name, Middle_Initial, Last_Name, Gender, Email
def search():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    sqlQuery = f"""SELECT * FROM hundred WHERE Name_Prefix LIKE 'ms%'"""
    cursor.execute(sqlQuery)
    datos = cursor.fetchall()
    conn.commit()
    conn.close()
    for dato in datos:
        print(dato)


def updateData():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    sqlQuery = f"""UPDATE hundred SET Name_Prefix='Drs.' WHERE Emp_ID='407061' """
    cursor.execute(sqlQuery)
    conn.commit()
    conn.close()


# delete uno a uno
def deleteRow():
    conn=sql.connect("/home/gabriel/prog/analytics/csvSql/dbs/testdb.db")
    cursor=conn.cursor()
    sqlQuery = f"""DELETE FROM hundred WHERE Emp_ID='867084' """
    cursor.execute(sqlQuery)
    conn.commit()
    conn.close()



#createDB()
#createTable()
#insertRowData(867084, 'Ms.', 'Deborah', 'E', 'Smith', 'F', 'deborah.smith@yahoo.com')
#insertAllData()

# fields === Emp_ID, Name_Prefix, First_Name, Middle_Initial, Last_Name, Gender, Email
readOrdered("Emp_ID")

#search()
#updateData()

# (867084, 'Ms.', 'Deborah', 'E', 'Smith', 'F', 'deborah.smith@yahoo.com')
#deleteRow()


