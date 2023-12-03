import gzip
import json
from sqlite3 import Row

from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession, types, functions
import pyspark.sql.functions as psf


from rake_nltk import Rake

assert sys.version_info >= (3, 0) # make sure we have Python 3.5+


def main():
    reviews_schema = types.StructType([
        types.StructField('reviewerID', types.StringType()),
        types.StructField('verified', types.BooleanType())
    ])
    office_products = spark.read.schema(reviews_schema).json("reviews/Office_Products_5.json.gz")
    clothing_shoes = spark.read.schema(reviews_schema).json("reviews/Clothing_Shoes_and_Jewelry_5.json.gz")
    movies_tv = spark.read.schema(reviews_schema).json("reviews/Movies_and_TV_5.json.gz")

    office_products = office_products.where(office_products['verified'] == True)
    clothing_shoes = clothing_shoes.where(clothing_shoes['verified'] == True)
    movies_tv = movies_tv.where(movies_tv['verified'] == True)

    office_products_freq = office_products.groupBy("reviewerID").count().withColumnRenamed('count', 'office_products_count')
    clothing_shoes_freq = clothing_shoes.groupBy("reviewerID").count().withColumnRenamed('count', 'clothing_count')
    movies_tv_freq = movies_tv.groupBy("reviewerID").count().withColumnRenamed('count', 'movies_count')
    joined = office_products_freq.join(clothing_shoes_freq, on='reviewerID', how='outer')
    joined = joined.join(movies_tv_freq, on='reviewerID', how='outer').na.fill(value=0)
    cond = "psf.when" + ".when".join(["(psf.col('" + c + "') == psf.col('max_value'), psf.lit('" + c + "'))" for c in joined.columns])
    joined_max = joined.withColumn("max_value", psf.greatest(*joined.columns[1:4])) \
        .withColumn("MAX", eval(cond))
    joined_max.withColumn("most_often", functions.split(joined_max.MAX, '_')[0]) \
        .show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    main()