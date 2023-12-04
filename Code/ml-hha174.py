import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

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
                                     

def main(inputs, output):
    # main logic starts here
    cat_name = 'Movies'
    meta_data = spark.read.json('meta_' + cat_name + '*', schema=meta_schema)
    meta_data.show()
    
    avg = spark.read.json('avg-' + cat_name.lower() , schema=avg_schema)
    avg.show()
    
    data = avg.join(meta_data, 'asin')
    data = data.where(data['price'].startswith('$'))
    data = data.where(data['brand'].isNotNull() & (functions.length(data['brand']) > 0))
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
    
    model.write().overwrite().save(cat_name.lower() + '-model')
    
    predictions = model.transform(validation)
    predictions.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='num_purchase',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)    
        
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
