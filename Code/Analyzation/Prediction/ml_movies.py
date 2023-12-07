import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
from plotly import express as px

# add more functions as necessary
avg_schema = types.StructType([
                    types.StructField('asin', types.StringType()),
                    types.StructField('weighted_avg', types.DoubleType()),
                    types.StructField('num_purchase', types.IntegerType())
                    ])
                    
meta_schema =  types.StructType([
                    types.StructField('asin', types.StringType()),
                    types.StructField('title', types.StringType()),
                    types.StructField('price', types.StringType()),
                    types.StructField('brand', types.StringType())
                    #types.StructField('categories', types.ArrayType(types.ArrayType(types.StringType())))
                    ])  

review_schema = types.StructType([
                    types.StructField('overall', types.DoubleType()),
                    types.StructField('asin', types.StringType()),
                    types.StructField('vote', types.StringType()),
                    types.StructField('image', types.ArrayType(types.StringType(), False)),
                    types.StructField('verified',types.BooleanType()) 
                    ])                                     
                                     

def main():
    # add more functions as necessary
    
                    
    weight_image = 1
    weight_vote = 1    
   
    # main logic starts here
    
    reviews_path = '/Users/jarvis/Amazon_Product_Analysis/Dataset/Movies_and_TV_5.json.gz'
    reviews = spark.read.json(reviews_path, schema=review_schema)
    reviews = reviews.where(reviews['verified'])
    reviews = reviews.withColumn('count', functions.lit(1))
    
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
    
    meta_path = '/Users/jarvis/Amazon_Product_Analysis/Dataset/meta_Movies_and_TV.json.gz'
    meta_data = spark.read.json(meta_path, schema=meta_schema)
    meta_data = meta_data.fillna(value = '0', subset = ['price'])
    meta_data.show()
    
    avg = weighted_avg
    avg.show()
    
    data = avg.join(meta_data, 'asin')
    data = data.where(data['price'].startswith('$'))
    data = data.where(data['brand'].isNotNull() & (functions.length(data['brand']) > 0))
    data = data.fillna(value = 0)
    data.show()

    
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    # TODO: create a pipeline to predict product price, avg_rating, brand -> num_purchase
    
    #brand_indexer = StringIndexer(inputCol="brand", outputCol="brand_idx")
    same_brand_purchase = SQLTransformer(
        statement=
            "SELECT DISTINCT __THIS__.*, avg_data.same_brand_purchase \
             FROM __THIS__ \
             JOIN ( \
             SELECT brand, AVG(num_purchase) AS same_brand_purchase \
             FROM __THIS__ \
             GROUP BY brand \
             ) avg_data ON __THIS__.brand = avg_data.brand")
    
    price_in_num = SQLTransformer(statement="SELECT *, CAST(SUBSTRING_INDEX(price,'$',-1) AS FLOAT) AS price_num FROM __THIS__")
    
    assembler = VectorAssembler(
        inputCols=['price_num', 'weighted_avg', 'same_brand_purchase'],
        outputCol='features')
    assembler.setHandleInvalid("skip")
    #score 0.7080414366465715    
    lr = LinearRegression(
        regParam=0.5, 
        solver="normal",
        featuresCol = 'features',
        labelCol = 'num_purchase',
        predictionCol = 'prediction'
        )    
     
    #score 0.3988248628069814    
    gbt = GBTRegressor(
        featuresCol = 'features',
        labelCol = 'num_purchase',
        predictionCol = 'prediction'
        )
    
    #score 0.5349271438178226
    rf = RandomForestRegressor(  
        numTrees=8,
        maxDepth=8,
        featuresCol = 'features',
        labelCol = 'num_purchase',
        predictionCol = 'prediction',
        maxBins = 256) 
    
           
        
    pipeline = Pipeline(stages=[same_brand_purchase, price_in_num, assembler, lr])
    
    model = pipeline.fit(train)
    
    
    
    predictions = model.transform(validation)
    predictions.select('title','num_purchase','prediction').show()
    
    #plotting
    real = predictions.select('asin','num_purchase')
    real = real.withColumn('type', functions.lit('actual'))
    predicted = predictions.select('asin',predictions['prediction'].alias('num_purchase'))
    predicted = predicted.withColumn('type', functions.lit('prediction'))
    predicted.show()
    
    
    pd_merged = pd.concat([real.limit(1000).toPandas(), predicted.limit(1000).toPandas()], ignore_index=True, sort=False)
    pd_merged.to_csv("/Users/jarvis/Amazon_Product_Analysis/Results/ml_movies.csv")
    #fig = px.histogram(pd_merged, x = 'num_purchase', color = 'type', histnorm = 'percent')
    #fig.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='num_purchase',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)    
        
if __name__ == '__main__':
    spark = SparkSession.builder.appName('Predict Purchases').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()