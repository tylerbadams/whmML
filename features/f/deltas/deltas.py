from pymongo import MongoClient
import datetime

"""
Deltas Feature Generation Script

Author: Tyler B Adams
Nucleation Date: 2017-06-14

The Deltas generation script will calculate the % change in price over a period of time.
It will then add that feature to two documents as an input variable (on the previous record
identified for the analysis) and an output feature (on the current record)

"""

database = 'financial'
collection = 'ticker'
client = MongoClient('localhost', 27017)
db = client[database]

timedeltas = [10, 60, 120, 480, 960, 1920, 7680]

records = db[collection].find({'^%delta -10min':  { '$exists': False} })

for z,entry in enumerate(records):
    print '{0}/{1}'.format(str(z), str(z))
    for t in timedeltas:
            tme = datetime.datetime.now()
            r = db[collection].find({'ticker': entry['ticker'], 'timestamp':{'$gte': (tme - datetime.timedelta(minutes=t)) , '$lte': (tme - datetime.timedelta(minutes = (t - 5)))}} ).sort('timestamp').limit(2)
            for i,x in enumerate(r):
                try:
                    vdelta = ((entry['lastVolume'] - x['lastVolume'])/x['lastVolume']) * 100
                    entry['^%lvdelta -{0}min'.format(str(t))] = vdelta
                    x['*%lvdelta +{0}min'.format(str(t))] = vdelta
                except:
                    pass

                try:
                    pdelta = ((entry['price'] - x['price'])/x['price']) * 100
                    entry['^%delta -{0}min'.format(str(t))] = pdelta
                    x['*%delta +{0}min'.format(str(t))] = pdelta
                except:
                    pass
                db[collection].update({'_id': x['_id']}, x)
    db[collection].update({'_id': entry['_id']}, entry)



