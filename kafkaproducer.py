'''	
@Author: Aishwarya
@Date: 2022-01-19
@Title : A  producer sending messages message to consumer 
'''
#########################################################################################
from kafka import KafkaProducer

import requests
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('python-kafka',b'lets you python to execute msgs')
producer.send('python-kafka',b'This is Kafka-Python, Here we are using only single broker')
producer.send('python-kafka',b'This is Kafka-Python, Message from producer')
producer.flush()
