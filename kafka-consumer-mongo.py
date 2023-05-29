# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
import subprocess



# replace here with your mongodb url 
uri = "mongodb+srv://sagnnax:sanate@nosql.hhmeowd.mongodb.net/?retryWrites=true&w=majority"


# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.nosql
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")

consumer = KafkaConsumer('comments',bootstrap_servers=[
     'my-kafka-0.my-kafka-headless.fhatimareyes.svc.cluster.local:9092'
    ])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    userId = record["userId"]
    objectId = record["objectId"]
    comment = record["comment"]

    # Create dictionary and ingest data into MongoDB
    try:
        comment_rec = {
            'userId': userId,
            'objectId': objectId,
            'comment': comment
        }
        print(comment_rec)
        comment_id = db.nosql_comments.insert_one(comment_rec)
        print("Comment inserted with record ids", comment_id)
    except Exception as e:
        print("Could not insert into MongoDB:")

    # Create bdnosql_sumary and insert groups into mongodb
    try:
        agg_result = db.nosql_comments.aggregate([
              {
         "$group": {
                "_id": {
                    "objectId": "$objectId",
                    "comment": "$comment"
                },
                "n": {"$sum": 1}
            }
        }
    ])

        db.nosql_sumaryComments.delete_many({})
        for i in agg_result:
            print(i)
            sumaryComments_id = db.nosql_sumaryComments.insert_one(i)
            print("Sumary Comments inserted with record ids: ", sumaryComments_id)
    except Exception as e:
        print(f'group vy cought {type(e)}: ')
        print(e)