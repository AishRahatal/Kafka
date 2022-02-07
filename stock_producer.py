
'''	
@Author: Aishwarya
@Date: 2022-01-20
@Title : producer code for publishing stock data sending to consumer
'''
#########################################################################################

import csv
import requests
import os
import time
from kafka import KafkaProducer
from json import dumps

if __name__=='__main__':
   

    bootstrap_servers = ['localhost:9092']
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer=lambda K:dumps(K).encode('utf-8'))

    data = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey=TVGXM12NYN5T3X5K'

    with requests.Session() as s:
        download = s.get(data)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        for row in my_list:
            producer.send('stock',row)
            time.sleep(1)
            
        
