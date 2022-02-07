'''	
@Author: Aishwarya
@Date: 2022-01-20
@Title : Reading content from link and send to consumer
'''
#########################################################################################
import csv
import requests
import os
import time
from kafka import KafkaProducer
from json import dumps

bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer=lambda K:dumps(K).encode('utf-8'))

data = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey=TVGXM12NYN5T3X5K'


with requests.Session() as s:
    download = s.get(data)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        producer.send('testTopic',row)
