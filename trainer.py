import numpy as np
import pymongo as MongoClient

client = MongoClient.MongoClient('localhost')
db = client.tarsiVR
for cName in db.collection_names():
    if '_raw' in cName:
        for classifier in db[cName].distinct('classifier'):
            data = []
            means = []
            sps = []
            for raw in db[cName].find({'classifier': classifier}, {'inputs': 1}):
                data.append(raw['inputs'])
            data = np.array(data)

            for col in range(np.shape(data)[1]):
                means.append(np.mean(data[:,col]))
                sps.append(np.std(data[:,col]))

            rName = cName.replace('_raw', '_result')
            db[rName].update({'classifier': classifier},
                             {'classifier': classifier,
                              'means': means,
                              'sps': sps},
                             upsert = True )
