import requests
import json
from datetime import datetime, date
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def data_crawling():
    crawling_from = requests.post('https://api.openweathermap.org/data/2.5/weather?q=%22+namakota+%22&appid=d5f11f21e3d4617bc66950d463fdeb4b')
    output = crawling_from.json()
    result = {}

    # datetime
    current_datetime = datetime.now()
    date_today = date.today()
    date_now = date_today.strftime('%y-%m-%d')

    # time
    timestamp = datetime.timestamp(current_datetime)
    initial_timestamp = datetime.fromtimestamp(timestamp)
    timestamp_formatting = initial_timestamp.isoformat(timespec='microseconds')
    time_now = current_datetime.strftime('%h:%m:%s')

    # obtained data
    result['city_name'] = output["name"]
    result['longitude'] = output["coord"]["lon"]
    result['latitude'] = output["coord"]["lat"]
    result['weather_status'] = output["weather"]["main"]
    result['weather_description'] = output["weather"]["description"]
    result['temperature'] = output["main"]["temp"]
    result['temperature_min'] = output["main"]["temp_min"]
    result['temperature_max'] = output['main']['temp_max']
    result['humidity'] = output['main']['humidity']

    result['date'] = date_now
    result['time'] = time_now
    result['created'] = timestamp_formatting

    return output

def data_calling():
    city_list = ["Aceh, Palembang, Jakarta, Surabaya, Bali, dan Papua"]
    for city in city_list:
        data_call = data_crawling(city)
        producer.send('weather_report', value=data_call)
        print(data_call)

while True:
    data_calling()
    time.sleep(1800)