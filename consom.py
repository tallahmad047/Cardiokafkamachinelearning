import json 
import pandas as pd
from time import sleep
from datetime import date, datetime
from kafka import KafkaConsumer
from pycaret.classification import load_model
from pycaret.classification import*
import pymongo


conn_str = "mongodb://localhost:27017/"


try:
    client = pymongo.MongoClient(conn_str)
except Exception as e:
    print("Error: ", str(e))


# # Create DataBase Section

myDb = client["stream_medical_data"]

# # Create DataBase Collection

myCollection =myDb["cardiac_failure"]

# path name must be same with producer.
model = load_model("code")

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'Heart_Failure_Project', # topic name !!!!
        bootstrap_servers='localhost:9092',
        group_id = None,
        auto_offset_reset='earliest')

    for message in consumer:
        # firstly load to consumer data then transfer to pandas dataframe
        stream_data = pd.DataFrame(pd.read_json(json.loads(message.value),typ="dict")).T
        print(stream_data)
        