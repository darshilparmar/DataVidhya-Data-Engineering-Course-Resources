from bikes import Bike
from constants import BIKES_STATION_INFORMATION, BIKES_STATION_STATUS

while True:
    bike = Bike()

    bike.get_bikes_station_information(BIKES_STATION_INFORMATION,{})
    bike.get_bikes_station_status(BIKES_STATION_STATUS, {})


