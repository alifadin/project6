from kafka import KafkaConsumer
from json import loads
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "mongodb+srv://alifadinramadhan182:mongoDB182@cluster-digitalskola.gsjqe5u.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

consumer = KafkaConsumer(
    'finnhub',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',   
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    group_id='consumer-id-1')

mydb = client["finnhub"]
mycol = mydb["customers"]
while True : 
    for message in consumer:
        message_raw = message.value
        msg = loads(message_raw)
        if msg['type'] == 'trade' :
            for res in msg ['data'] :
                mycol.insert_one(res)
