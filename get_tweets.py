from requests_oauthlib import OAuth1
import os
import requests
from env_vars import cons_key, cons_sec, acc_key, acc_sec
from pykafka import KafkaClient


def get_tweets(CK=None, CS=None, AT=None, AS=None, parameters=None, hosts=None, kafka_topic=None):
    # Get a stream of realtime tweets

    # Twitter API
    auth = OAuth1(CK, CS, AT, AS)
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    response = requests.get(url, auth=auth, params=parameters, stream=True)

    # Kafka Producer
    client = KafkaClient(hosts=hosts)
    topic = client.topics[kafka_topic]

    # Generate a loop to get responses
    with topic.get_sync_producer() as producer:
        for line in response.iter_lines():
            if line:
                producer.produce(line)


if __name__ == '__main__':
    # Define environment variables
    CK = cons_key
    CS = cons_sec
    AT = acc_key
    AS = acc_sec

    # Twitter API parameters
    parameters = {"stall_warnings": "true",
                  "locations": "-99.26,19.19,-98.98,19.53"}

    while True:
        try:
            get_tweets(CK=CK, CS=CS, AT=AT, AS=AS,
                       parameters=parameters, hosts="127.0.0.1:9092", kafka_topic="twitter")
        except Exception as e:
            print(e)
