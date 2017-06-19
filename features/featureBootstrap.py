import json
import os
import time
rootDir = '/var/www/inertia/features'
with open('{0}/{1}'.format(rootDir, 'conf.json')) as myConf:
    featureConf = json.load(myConf)

for script in featureConf['scripts']:
    t0 = time.time()
    print script
    os.system('python {0}/{1}'.format(featureConf['rootDir'], script['wrapper']))
    print time.time() - t0
