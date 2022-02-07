'''	
@Author: Aishwarya
@Date: 2022-01-20
@Title : Consumer code for consuming stock data and storing into the hdfs
'''
#########################################################################################
from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
import subprocess

if __name__=='__main__':
    consumer = KafkaConsumer( 'stock')
    #hadoop folder path
    hdfs.mkdir('hdfs://localhost:9000/stock/datakafka')
    #path of local empty file
    file='/home/hadoop/python_codes/kafka/data.txt'
    #putting file into hadoop
    args = [ 'hdfs','dfs','-put',file,'/stock/datakafka' ]
    print('Running system command :{}'.format(' '.join(args)))
    proc = subprocess.Popen(args,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    path='hdfs://localhost:9000/stock/datakafka/data.txt'
    for message in consumer:
        value = message.value.decode('utf-8')
        #print(values)
        with hdfs.open(path,'at') as f:
        #writing into hdfs file as producer sending message
            f.write(f"{value}\n")
