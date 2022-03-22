import sys
import argparse

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, col, lit, when


def _split_review_str_into_words(df):
    tokenizer = Tokenizer(outputCol="words")
    tokenizer.setInputCol("review_str")
    return tokenizer.transform(df)


def _remove_stop_words(df):
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(stopWords=stop_words)
    remover.setInputCol("words")
    remover.setOutputCol("clean_words")
    return remover.transform(df)


def _classify_review_sentiment(df):
    return df.withColumn("positive_review", array_contains(col("clean_words"), "good"))


def _change_sentiment_to_int(df):
    return df.withColumn(
        "positive_review", when(col("positive_review") == lit(True), 1).otherwise(0)
    )


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True)
    parser.add_argument("--output_path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    flags = _parse_args(sys.argv[1:])

    # Setup packages to write avro files, they need to have the same version as
    # spark
    spark = (
        SparkSession.builder.appName("Movies Reviews")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.1")
        .getOrCreate()
    )

    movies_reviews_df = spark.read.option("header", True).csv(flags.input_file)

    movies_reviews_df = _split_review_str_into_words(movies_reviews_df)
    movies_reviews_df = _remove_stop_words(movies_reviews_df)
    movies_reviews_df = _classify_review_sentiment(movies_reviews_df)
    movies_reviews_df = _change_sentiment_to_int(movies_reviews_df)
    movies_reviews_df = movies_reviews_df.select("cid", "id_review", "positive_review")
    movies_reviews_df.write.format("avro").save(flags.output_path)
