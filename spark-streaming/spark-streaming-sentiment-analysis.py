from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from textblob import TextBlob
import re
import string
import emoji
import nltk

nltk.download('punkt')
nltk.download('wordnet')

@udf
def preprocess_twitter_text(text):
    # Convert to lowercase
    text = text

    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)

    # Remove mentions and hashtags
    text = re.sub(r'@\w+|\#\w+', '', text)

    # Remove special characters and punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Remove emojis
    text = emoji.demojize(text)
    text=re.sub('(:[a-zA-Z_]+:)','',text)

    return text


@udf
def analyze_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

if __name__=="__main__":
    spark=SparkSession.builder\
        .appName("Spark Sentiment Analysis")\
        .config("spark.master","local[*]") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("link", StringType(), True),
        StructField("content", StringType(), True),
        StructField("date", StringType(), True),
        StructField("retweets", IntegerType(), True),
        StructField("favorites", IntegerType(), True),
        StructField("mentions", StringType(), True),
        StructField("hashtags", StringType(), True)
    ])

    kafka_topic = "tweets_topic"
    kafka_broker = "localhost:9092"

    kafka_df=spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

    tweets_df=value_df.selectExpr("value.*")

    # tweets_df.printSchema()

    # hashtags=tweets_df.select("hashtags")

    content_df=tweets_df.select("content")

    # content_df.printSchema()


    preprocessed_text_df=content_df.withColumn("preprocessed_text",preprocess_twitter_text('content'))

    analyze_sentiment_df=preprocessed_text_df.withColumn("sentiment_score",analyze_sentiment('preprocessed_text'))

    # preprocessed_text_df.printSchema()
    analyze_sentiment_df=analyze_sentiment_df.select("content","sentiment_score")

    query = analyze_sentiment_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

    # content_df.show()

