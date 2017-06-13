from pymongo import MongoClient
import datetime
import numpy as np
import time

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
for ticker in db[collection].find( {'timestamp': {'$gte': timeWindow } } ).distinct( 'ticker' ):
    payload = [res for res in db[collection].find({'ticker': ticker, 'timestamp': { '$gte': timeWindow } } )]
    for entry in payload:
        can = {}
        for p in periods:
            resL = db[collection].find({'ticker': ticker, 'timestamp': {'$lte': entry['timestamp'], '$gte': entry['timestamp'] - datetime.timedelta(minutes = (p['long']))}})
            resL = np.mean([r['price'] for r in resL])
            
            resS = db[collection].find({'ticker': ticker, 'timestamp': {'$lte': entry['timestamp'], '$gte': entry['timestamp'] - datetime.timedelta(minutes = (p['short']))}})
            resS = np.mean([r['price'] for r in resS])

            can['^macd_{0}_{1}'.format(str(p['long']), str(p['short']))] = (resL - resS)/entry['price'] * 100
            
        db[collection].update_one({'_id': entry['_id']}, {'$set': can})
