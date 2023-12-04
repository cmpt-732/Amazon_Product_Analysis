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

def main():
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

            office_reviews = spark.read.schema(reviews_schema).json("reviews/Office_Products_5.json.gz").select("asin","reviewText","overall").cache()
            movies_reviews = spark.read.schema(reviews_schema).json("reviews/Movies_and_TV_5.json.gz").select("asin","reviewText","overall").cache()
            clothing_reviews = spark.read.schema(reviews_schema).json("reviews/Clothing_Shoes_and_Jewelry_5.json.gz").select("asin","reviewText","overall").cache()

            office_good_reviews = office_reviews.filter(office_reviews["overall"] >= 4.0)
            office_bad_reviews = office_reviews.filter(office_reviews["overall"] <= 3.0)
            get_keywords_udf = udf(get_keywords, ArrayType(types.StructType([types.StructField("score", FloatType(), False),
    types.StructField("keywords", StringType(), False)])))
            office_good_reviews = office_good_reviews.filter(office_good_reviews["reviewText"].isNotNull())
            office_good_reviews = office_good_reviews.withColumn("key_words", get_keywords_udf(office_good_reviews["reviewText"]))
            office_bad_reviews = office_bad_reviews.filter(office_bad_reviews["reviewText"].isNotNull())
            office_bad_reviews = office_bad_reviews.withColumn("key_words", get_keywords_udf(office_bad_reviews["reviewText"]))
            office_good_reviews.limit(500).toPandas()
            office_bad_reviews.limit(500).toPandas()

            office_good_reviews.show(20)

            movies_good_reviews = movies_reviews.filter(movies_reviews["overall"] >= 4.0)
            movies_bad_reviews = movies_reviews.filter(movies_reviews["overall"] <= 3.0)
            get_keywords_udf = udf(get_keywords,
                                   ArrayType(types.StructType([types.StructField("score", FloatType(), False),
                                                               types.StructField("keywords", StringType(), False)])))
            movies_good_reviews = movies_good_reviews.filter(movies_good_reviews["reviewText"].isNotNull())
            movies_good_reviews = movies_good_reviews.withColumn("key_words",
                                                                 get_keywords_udf(movies_good_reviews["reviewText"]))
            movies_bad_reviews = movies_bad_reviews.filter(movies_bad_reviews["reviewText"].isNotNull())
            movies_bad_reviews = movies_bad_reviews.withColumn("key_words",
                                                               get_keywords_udf(movies_bad_reviews["reviewText"]))
            movies_good_reviews.limit(500).toPandas()
            movies_bad_reviews.limit(500).toPandas()

            movies_good_reviews.show(20)

            clothing_good_reviews = clothing_reviews.filter(clothing_reviews["overall"] >= 4.0)
            clothing_bad_reviews = clothing_reviews.filter(clothing_reviews["overall"] <= 3.0)
            get_keywords_udf = udf(get_keywords,
                                   ArrayType(types.StructType([types.StructField("score", FloatType(), False),
                                                               types.StructField("keywords", StringType(), False)])))
            clothing_good_reviews = clothing_good_reviews.filter(clothing_good_reviews["reviewText"].isNotNull())
            clothing_good_reviews = clothing_good_reviews.withColumn("key_words",
                                                                 get_keywords_udf(clothing_good_reviews["reviewText"]))
            clothing_bad_reviews = clothing_bad_reviews.filter(clothing_bad_reviews["reviewText"].isNotNull())
            clothing_bad_reviews = clothing_bad_reviews.withColumn("key_words",
                                                               get_keywords_udf(clothing_bad_reviews["reviewText"]))

            clothing_good_reviews.limit(500).toPandas()
            clothing_bad_reviews.limit(500).toPandas()

            clothing_good_reviews.show(20)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('example code').config("spark.sql.execution.arrow.pyspark.enabled", "false").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+

    main()