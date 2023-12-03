import gzip
import json
from sqlite3 import Row

from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, FloatType

from rake_nltk import Rake

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+



# add more functions as necessary
def get_keywords(x):
    r = Rake()
    r.extract_keywords_from_text(x)
    return r.get_ranked_phrases_with_scores()

def main(inputs):
            reviews_schema = types.StructType([
                types.StructField('reviewerID', types.StringType()),
                types.StructField('vote', types.StringType()),
                types.StructField('asin', types.StringType()),
                types.StructField('reviewerName', types.StringType()),
                types.StructField('helpful', types.StringType()),
                types.StructField('reviewText', types.StringType()),
                types.StructField('overall', types.FloatType()),
                types.StructField('summary', types.StringType()),
                types.StructField('unixReviewTime', types.IntegerType()),
                types.StructField('reviewTime', types.StringType()),
            ])

            reviews = spark.read.schema(reviews_schema).json("reviews/{file}".format(file=inputs)).select("asin","reviewText","overall").cache()
            good_reviews = reviews.filter(reviews["overall"] >= 4.0)
            bad_reviews = reviews.filter(reviews["overall"] <= 3.0)
            get_keywords_udf = udf(get_keywords, ArrayType(types.StructType([types.StructField("score", FloatType(), False),
    types.StructField("keywords", StringType(), False)])))
            good_reviews = good_reviews.filter(good_reviews["reviewText"].isNotNull())
            good_reviews = good_reviews.withColumn("key_words", get_keywords_udf(good_reviews["reviewText"]))
            bad_reviews = bad_reviews.filter(bad_reviews["reviewText"].isNotNull())
            bad_reviews = bad_reviews.withColumn("key_words", get_keywords_udf(bad_reviews["reviewText"]))
            # good_reviews = good_reviews.select("asin", functions.concat_ws(", ", functions.col("key_words").cast("array<string>")).alias("key_words"))
            good_reviews.toPandas()
            bad_reviews.toPandas()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('example code').config("spark.sql.execution.arrow.pyspark.enabled", "false").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)