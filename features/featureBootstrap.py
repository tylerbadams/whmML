import json
import os

with open('conf.json') as myConf:
    featureConf = json.load(myConf)

for script in featureConf['scripts']:
    print script
    os.system('python {0}/{1}'.format(featureConf['rootDir'], script['wrapper']))
