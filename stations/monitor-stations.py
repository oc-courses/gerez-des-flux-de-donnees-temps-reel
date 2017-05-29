#! /usr/bin/env python3
import json
from kafka import KafkaConsumer

stations = {}
consumer = KafkaConsumer("velib-stations", bootstrap_servers='localhost:9092',
                         group_id="velib-monitor-stations")
for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
    available_bikes = station["available_bikes"]

    if contract not in stations:
        stations[contract] = {}
    city_stations = stations[contract]

    if station_number not in city_stations:
        city_stations[station_number] = 0

    count_diff = available_bikes - city_stations[station_number]
    if count_diff != 0:
        city_stations[station_number] = available_bikes
        print("{}{} bike at station {} ({})".format(
            "+" if count_diff > 0 else "",
            count_diff, station["address"], contract
        ))
