import requests
import datetime
from pymongo import MongoClient
import json
import itertools


with open('config.json') as config_h:
    config = json.load(config_h)
    symbols = config['symbols']
    db = config['db']

threshold = 10
client = MongoClient(db['host'], db['port'])
db = client[db['name']]
api_url = "https://min-api.cryptocompare.com/data/pricemultifull?"

timedeltas = [5, 10, 30, 60, 120, 240, 480, 960, 1920, 3840, 7680]
collection  = 'ticker'

def format(entry):
	# TODO : mongo should have its own created timestamps
	time = datetime.datetime.now()
	name = entry[0]
 	price = float(entry[1]['PRICE'])
 	lastVolumeTo = float(entry[1]['LASTVOLUMETO'])
 	lastVolume = float(entry[1]['LASTVOLUME'])

	entry = {
			'timestamp': time ,
			 'ticker': entry[0],
			 'price': price,
			 'lastVolumeTo' : lastVolumeTo,
			 'lastVolue' : lastVolume
			}
	return entry


# Split ticker into groups of threshold in order to play nice with cryptocompare server
args = [iter(symbols)] * threshold
symbols_for_dispatch = itertools.izip_longest(fillvalue='', *args)
urls = [api_url + 'fsyms=USD&tsyms={0}'.format(','.join(temp)) for temp in symbols_for_dispatch]

data = list(itertools.chain.from_iterable([requests.get(url).json()["RAW"]['USD'].items() for url in urls]))
[db[collection].insert_one(format(entry)) for entry in data]
