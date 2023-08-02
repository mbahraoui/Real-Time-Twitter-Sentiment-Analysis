import asyncio
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "tweets_topic"
TWEET_STREAM_URL = "http://localhost:8000/tweet/stream"

# Kafka producer configuration
producer_config = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "client_id": "tweet_producer",
    "value_serializer": lambda v: v.encode("utf-8"),
}


async def fetch_tweets_and_produce():
    producer = KafkaProducer(**producer_config)

    response = requests.get(TWEET_STREAM_URL, stream=True)
    for line in response.iter_lines():
        if line:
            tweet_data = line.decode("utf-8")
            producer.send(KAFKA_TOPIC, value=tweet_data)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_tweets_and_produce())
