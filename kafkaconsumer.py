'''	
@Author: Aishwarya
@Date: 2022-01-19
@Title : A  consumer recieving message from producer 
'''
#########################################################################################
from kafka import KafkaConsumer

consumer = KafkaConsumer('python-kafka')
for message in consumer:
    print (message)
