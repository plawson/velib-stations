import json
import time
import urllib.request
import argparse

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--apikey', required=True, help='provide your API key')

args = parser.parse_args()

API_KEY = args.apikey  # FIXME Set your own API key here
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093"])

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode(),
                      key=str(station["number"]).encode())
    print("Produced {} station records".format(len(stations)))
    time.sleep(1)
