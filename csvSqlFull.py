import sqlite3

def csv_sql(file_dir,table_name,database_name):
    con = sqlite3.connect(database_name)
    cur = con.cursor()
    # Drop the current table by: 
    # cur.execute("DROP TABLE IF EXISTS %s;" % table_name)

    with open(file_dir, 'r') as fl:
        hd = fl.readline()[:-1].split(',')
        ro = fl.readlines()
        db = [tuple(ro[i][:-1].split(',')) for i in range(len(ro))]

    header = ','.join(hd)
    cur.execute("CREATE TABLE IF NOT EXISTS %s (%s);" % (table_name,header))
    cur.executemany("INSERT INTO %s (%s) VALUES (%s);" % (table_name,header,('?,'*len(hd))[:-1]), db)
    con.commit()
    con.close()



# Example:
csv_sql('/home/gabriel/prog/data_samples/100Records.csv','hundred','/home/gabriel/prog/analytics/csvSql/dbs/testdbsample.db')
