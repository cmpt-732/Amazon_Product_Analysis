import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
review_schema = types.StructType([
                    types.StructField('overall', types.DoubleType()),
                    types.StructField('asin', types.StringType()),
                    types.StructField('vote', types.StringType()),
                    types.StructField('image', types.ArrayType(types.StringType(), False)),
                    types.StructField('verified',types.BooleanType()) 
                    ])
                    
weight_image = 1
weight_vote = 1                    

def main(inputs, output):
    # main logic starts here
    cloumn_name = inputs.split('_')[0].lower()
    
    reviews = spark.read.json(inputs, schema=review_schema)
    reviews = reviews.where(reviews['verified'])
    reviews = reviews.withColumn('count', functions.lit(1))
    reviews.show()
    
    #vote
    reviews = reviews.withColumn('vote_value', reviews.vote.cast(types.IntegerType()))
    reviews = reviews.withColumn('num_vote', functions.coalesce(reviews.vote_value, functions.lit(0)))
    reviews = reviews.drop('vote_value')
    
    #image
    coalesced_array = functions.coalesce(reviews.image, functions.lit([]))
    reviews = reviews.withColumn('num_images', functions.size(
    coalesced_array)) 
    
    #weight
    reviews = reviews.withColumn('weight', functions.lit(1)+weight_image*reviews['num_images'] + weight_vote*reviews['num_vote'])
    #total_weight = reviews.groupBy().sum().collect()[0][0]
    
    #weighted average
    weighted_avg = reviews.groupBy('asin').agg(
        (functions.sum(reviews['overall']*reviews['weight'])/functions.sum(reviews['weight'])).alias('weighted_avg'),
        functions.count('count').alias('num_purchase'))
    weighted_avg.show(20)
    weighted_avg.write.json('avg-' + cloumn_name)
    
    movies = weighted_avg
    
    #max rating product
    best_product = weighted_avg.where(weighted_avg['num_purchase'] > 100)
    maxs = best_product.agg(functions.max(best_product['weighted_avg']).alias('max'))
    best_product = best_product.join(maxs)
    
    best_product = best_product.where(best_product['weighted_avg'] == best_product['max'])
    best_product.show()
    
    #write output to file
    result = best_product.select(best_product['asin'].alias(cloumn_name))
    result.show()
    result.write.json(output + '-' + cloumn_name)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
