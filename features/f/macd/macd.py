from pymongo import MongoClient
import datetime
import numpy as np

"""
Moving Average Convergence Divergence(MACD) Feature Generator

Author: Tyler B Adams
Nucleation Date: 2017-06-12

The MACD feature generator will cacluate the MACD value for a number of long and short set points
and add them as attributes to each ticker document in the financial.ticker collection.

Output attributes are designed for use as predictors (model inputs) in the system.

For algorithm information see this link: https://en.wikipedia.org/wiki/MACD
"""

database = 'financial'
collection = 'ticker'
client = MongoClient('localhost', 27017)
db = client[database]

periods = [{'long': 60, 'short': 30},
          {'long': 120, 'short': 60},
          {'long': 240, 'short': 120},
          {'long': 480, 'short': 240},
          {'long': 960, 'short': 480},
          {'long': 1920, 'short': 960},
          {'long': 3840, 'short': 1920},
          {'long': 7680, 'short': 3840}
]
timeWindow = datetime.datetime.now() - datetime.timedelta(minutes = periods[-1]['long'])
attempts = 2 

for p in periods:
    print p
    attr = '^macd_{0}_{1}'.format(str(p['long']), str(p['short']))
    count =  db[collection].find({ attr: {'$exists': False} , '$or': [{'%macdAttempts': {'$exists': False}},
                                               { '%macdAttempts': {'$lte': attempts}}]}).count()

    #find entries that are missing the the feature
    documents = db[collection].find({ attr: {'$exists': False} , '$or': [{'%macdAttempts': {'$exists': False}},
                                               { '%macdAttempts': {'$lte': attempts}}]})
    for i,doc in enumerate(documents):
        dataset = db[collection].find({'ticker': doc['ticker'], 'timestamp': {'$lte': doc['timestamp'], '$gte': doc['timestamp'] - datetime.timedelta(minutes = (p['long']))}}).sort('timestamp')
        dataset = [d for d in dataset]
        print '{2}/{3}  -  {0}/{1}'.format(str(i), str(count), str(p['long']), str(len(dataset)))
        if dataset[0]['timestamp'] < (doc['timestamp'] - datetime.timedelta(minutes = (p['long'] - p['long'] * 0.1))):
            sData = []
            lData = [d['price'] for d in dataset]
            for d in dataset:
                if d['timestamp'] >= (doc['timestamp'] - datetime.timedelta(minutes = p['short'])):
                    sData.append(d['price'])
            
            sMean = np.mean(sData)
            lMean = np.mean(lData)           
            db[collection].update_one({'_id': doc['_id']}, { '$set': {attr: (lMean - sMean) / doc['price'] * 100}})
        else:
            if '%macdAttempts' not in doc:
                doc['%macdAttempts'] = 0
            print doc['%macdAttempts']
            db[collection].update_one({'_id': doc['_id']}, {'$set': {'%macdAttempts': doc['%macdAttempts'] + 1}})
            print 'error'

