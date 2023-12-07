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

            office_reviews = spark.read.schema(reviews_schema).json("/Users/jarvis/Amazon_Product_Analysis/Dataset/Clothing_Shoes_and_Jewelry_5.json.gz").select("asin","reviewText","overall").cache()

            office_good_reviews = office_reviews.filter(office_reviews["overall"] >= 4.0)
            get_keywords_udf = udf(get_keywords, ArrayType(types.StructType([types.StructField("score", FloatType(), False),
    types.StructField("keywords", StringType(), False)])))
            office_good_reviews = office_good_reviews.filter(office_good_reviews["reviewText"].isNotNull())
            office_good_reviews = office_good_reviews.withColumn("key_words", get_keywords_udf(office_good_reviews["reviewText"]))
            office_good_reviews.select("key_words").limit(500).toPandas().to_json("/Users/jarvis/Amazon_Product_Analysis/Results/clothing_keywords.json")



if __name__ == '__main__':
    spark = SparkSession.builder.appName('example code').config("spark.sql.execution.arrow.pyspark.enabled", "false").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+

    main()
