from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import tweepy

client = tweepy.Client(
  consumer_key="AovCHQiJok70cBUW3hfwceLvc",
  consumer_secret="LP4P8AYADnpzjvPzCq7pWHMU9m1dw7JwsdtjzLcXqNAMlIqxZu",
  bearer_token="AAAAAAAAAAAAAAAAAAAAAE3uaAEAAAAAPKQflFXTGRUdQFNobLeYigGxMPk%3DP0HvsfD505U52v8Fzd7YH70uPFbkB1pAHsuT8oSPYgz6jOuiHq", 
  access_token="1265609384421457920-OkYPP56kOE3k4gcPn7XGzw4ffrpkrj", 
  access_token_secret="TsHAhpzMvxnTtCSOTIyPQ55gxnAYztwgWu7nd0jnAZTxn")

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# classification du texte
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words


if __name__ == "__main__":


    # création d'une session Spark
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

    # lecture des tweets à partir des sockets
    lines = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 5555).load()

    # traitement des données 
    words = preprocessing(lines)
    
    # traitement des données pour définir les populaires et les subjectives
    words = text_classification(words)

    words = words.repartition(1)
   
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("parquet")\
            .option("path", "./parc")\
            .option("checkpointLocation", "./check")\
            .trigger(processingTime='60 seconds').start()

    query.awaitTermination()
