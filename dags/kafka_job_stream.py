from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import time
import json
import uuid
from kconfig import config,sr_config
from json_schema_sample import schema_str 
from sample_movie import movies
from load_movie_data import get_trending_movies
def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'reading for {event.key().decode("utf8")} produced to {event.topic()}')
        
def movie_to_dict(movies,ctx):
    return dict(movies)   

def stream_to_kafka(topic,movies):
    

    schema_registry_client = SchemaRegistryClient(sr_config)
    
    json_serializer = JSONSerializer(schema_str, schema_registry_client,movie_to_dict) 
    
    producer = Producer(config)
    
    movieindex = 0
    movielength = len(movies)
    
    while movielength > movieindex:
        movieindex +=1
        for  data in movies:
            producer.produce(topic, key=str(uuid.uuid4()), 
                            value=json_serializer(data, 
                            SerializationContext(topic, MessageField.VALUE)),
                            callback=delivery_report)
            producer.flush()
            
        if movieindex == movielength:
            break
           
moviese = get_trending_movies()     
stream_to_kafka("latest_movie",moviese)