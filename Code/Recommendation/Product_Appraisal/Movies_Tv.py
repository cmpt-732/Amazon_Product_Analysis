import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from plotly import express as px
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

def main(inputs):
    # main logic starts here
    cloumn_name = inputs.split('_')[0].lower()
    
    reviews = spark.read.json(inputs, schema=review_schema)
    reviews = reviews.where(reviews['verified'])
    reviews = reviews.withColumn('count', functions.lit(1))
    #reviews.show()
    
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
    

    metadata = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('title', types.StringType())
    ]) 

    path = '/Users/hersh/Documents/BigDataLab/Project/meta_Movies_and_TV.json'
    metaDf = spark.read.json(path, schema = metadata)
    metaDf = metaDf.toDF('prod_id','product_name')
    best_product = weighted_avg.where(weighted_avg['num_purchase'] > 100)
    resultDF = best_product.join(metaDf, on = best_product.asin == metaDf.prod_id)
    output = resultDF.distinct()
    dfPur2 = output.groupBy(output.product_name).agg(functions.max('num_purchase').alias('Num_purchases'))
    pandDF3 = dfPur2.limit(100).toPandas()
    sorteddfPur2 = pandDF3.sort_values(by = ['Num_purchases'], ascending = False) 
    #print(sorteddfPur2.head(10))
    sorteddfPur2.to_csv("sorteddfPur2.csv")
    # fig3 = px.pie(sorteddfPur2, values = 'Num_purchases', names = 'product_name', title = 'Top 100 Customer Preferences in Movies and Tv', height = 1800, width = 2000)
    # fig3.show() 

    #Products with hightes weighted average:
    res2 = output.groupBy(output.product_name).agg(functions.max('weighted_avg').alias('final_weighted_avg'))
    pandDF4 = res2.limit(1000).toPandas()
    WeightedDf1 = pandDF4.sort_values(by = ['final_weighted_avg'], ascending = False)
    WeightedDf1.to_csv("WeightedDf1.csv")
    # fig4 = px.scatter(WeightedDf1, x = 'final_weighted_avg', y = 'product_name' ,title = 'Movies to recommend',height=800, width=2000)
    # fig4.update_layout(xaxis_title = 'Weighted Average', yaxis_title = 'Products')
    # fig4.show()
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)
