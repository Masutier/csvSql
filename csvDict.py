import csv


def readCsvDict(): # return simple dictionary
    with open("/home/gabriel/prog/data_samples/100Records.csv", newline = '') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter = ',')

        for row in csv_reader:
            print(row)


def readCsvDict2(): # return key and value
    with open("/home/gabriel/prog/data_samples/100Records.csv", newline = '') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter = ',')

        for row in csv_reader:
            for k, v, in row.items():
                print(k, v)


def readCsvDict3(): # return a desired item by key
    with open("/home/gabriel/prog/data_samples/100Records.csv", newline = '') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter = ',')

        for row in csv_reader:
            print(row["First_Name"], row["Middle_Initial"], row["Last_Name"])


readCsvDict()
readCsvDict2()
readCsvDict3()