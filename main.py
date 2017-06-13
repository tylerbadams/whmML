import requests
import datetime
from pymongo import MongoClient
import json

threshold = 10

with open('ticker.json') as myTicker:
    symbols = json.load(myTicker)['symbols']


timedeltas = [5, 10, 30, 60, 120, 240, 480, 960, 1920, 3840, 7680]



client = MongoClient('localhost', 27017)

db = client['financial']
collection  = 'ticker'
'''
for s in symbols:
    res = requests.get('https://min-api.cryptocompare.com/data/price?fsym={0}&tsyms=USD'.format(s))
    r = res.json()
    time = datetime.datetime.now()
    
    entry = {'timestamp': time , 'ticker': s, 'price': float(r['USD'])}

    for t in timedeltas:
        r = db[collection].find({'ticker': s, 'timestamp':{'$gte': (time - datetime.timedelta(minutes=t)) }} ).sort('timestamp').limit(1)
        for x in r:
            entry['*%delta -{0}min'.format(str(t))] = (entry['price'] - x['price'])/entry['price'] * 100
            x['*%delta +{0}min'.format(str(t))] = (entry['price'] - x['price'])/x['price'] * 100
            db[collection].update({'_id': x['_id']}, x)
    
    db[collection].insert_one(entry)
'''

while symbols:
    if len(symbols) > threshold:
        temp = symbols[0:threshold]
        symbols = symbols[threshold :]
    else:
        temp = symbols[0:]
        symbols = []
    print (temp)
    res = requests.get('https://min-api.cryptocompare.com/data/price?fsym=USD&tsyms={0}'.format(','.join(temp)))
    newData = res.json()

    for k in newData:
        time = datetime.datetime.now()

        entry = {'timestamp': time , 'ticker': k, 'price': 1./float(newData[k])}
        for t in timedeltas:
            r = db[collection].find({'ticker': k, 'timestamp':{'$gte': (time - datetime.timedelta(minutes=t)) }} ).sort('timestamp').limit(1)
            for x in r:
                entry['*%delta -{0}min'.format(str(t))] = (entry['price'] - x['price'])/entry['price'] * 100
                x['*%delta +{0}min'.format(str(t))] = (entry['price'] - x['price'])/x['price'] * 100
                db[collection].update({'_id': x['_id']}, x)

        db[collection].insert_one(entry)


