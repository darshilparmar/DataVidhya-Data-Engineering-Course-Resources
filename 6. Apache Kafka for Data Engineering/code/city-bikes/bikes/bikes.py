from services import HttpService
from kafka_producer import Producer
from constants import BIKES_STATIONS_INFORMATION_TOPIC, BIKES_STATION_STATUS_TOPIC

class Bike:
    def __init__(self):
        self.http_service = HttpService()
        self.producer = Producer()
    
    def get_bikes_station_information(self, url, params):
        res = self.http_service.get(url, params).json()
        for msg in res['data']['stations']:
            print("bike_station_information ==> ", msg)
            self.producer.data_producer(BIKES_STATIONS_INFORMATION_TOPIC, msg)

    def get_bikes_station_status(self, url, params):
        res = self.http_service.get(url, params).json()
        for msg in res['data']['stations']:
            print("bikes_station_status ==> ", msg)
            self.producer.data_producer(BIKES_STATION_STATUS_TOPIC, msg)