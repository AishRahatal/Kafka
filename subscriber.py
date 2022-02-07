'''	
@Author: Aishwarya
@Date: 2022-01-20
@Title : Consumer getting messages from producer and print message
'''
#########################################################################################
if __name__=='__main__':

	from kafka import KafkaConsumer
	consumer = KafkaConsumer( 'testTopic')
	for message in consumer:
		values = message.value
		print(values)
