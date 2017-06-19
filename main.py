import requests
import datetime
from pymongo import MongoClient
import json
import time

threshold = 20

with open('ticker.json') as myTicker:
    symbols = json.load(myTicker)['symbols']

client = MongoClient('localhost', 27017)

db = client['financial']
collection  = 'ticker'

while symbols:
    if len(symbols) > threshold:
        temp = symbols[0:threshold]
        symbols = symbols[threshold :]
    else:
        temp = symbols[0:]
        symbols = []

    res = requests.get('https://min-api.cryptocompare.com/data/pricemultifull?fsyms={0}&tsyms=USD'.format(','.join(temp)))
    newData = res.json()['RAW']
    tme = datetime.datetime.now()
    for k in newData:
        t = newData[k]['USD']
        entry = {'timestamp': tme , 
                 'ticker': k, 
                 'price': float(t['PRICE']),
                 'lastVolumeTo': float(t['LASTVOLUMETO']),
                 'lastVolume': float(t['LASTVOLUME']) }

        db[collection].insert_one(entry)
