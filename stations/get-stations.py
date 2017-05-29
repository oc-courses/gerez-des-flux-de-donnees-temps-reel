#! /usr/bin/env python3
import json
import time
import urllib.request

# Run `pip install kafka-python` to install this package
from kafka import KafkaProducer

API_KEY = "XXX" # FIXME
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode(),
                      key=str(station["number"]).encode())
    print("Produced {} station records".format(len(stations)))
    time.sleep(1)
