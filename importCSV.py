import happybase
import csv

## Setup HBase Connection
connection = happybase.Connection('127.0.0.1')

## CSV Info
directory = '/Users/steve/Documents/Data/NREL/Solar/1283/'
file = 'pvdaq_data_1283_1-1-2014_12-31-2014.csv'

## HBase Put Info
hbaseTableName = 'NREL'
hbaseCF = 'data'
rowkey = 'measdatetime'


table = connection.table(hbaseTableName)
b = table.batch()

connection.open()
print(table)

for key, data in table.scan(row_start='aaa'):
    print(key, data)

##table.put('row-key', {'cf:col1': 'value1', df:col2: 'value2'})

def getRowKey(r):
    #print(r)
    #print(hbaseCF + ":" + rowkey)
    return r.get(hbaseCF + ":" + rowkey)

def appendCF(d):
    return dict(map(lambda (key, value): (hbaseCF + ":" + str(key), value), d.items()))

def batchPut(record):
    b.put(getRowKey(record), record)


with open(directory + file, 'rb') as f:
    reader = csv.DictReader(f)
    for row in reader:
        formateddict = appendCF(row)
        batchPut(formateddict)

b.send()
