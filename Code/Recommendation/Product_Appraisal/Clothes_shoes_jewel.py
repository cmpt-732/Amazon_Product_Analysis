from pyspark.sql import SparkSession, functions, types
import sys
from plotly import express as px
assert sys.version_info >= (3,5)

def weightedImage(img):
    if img:
        return len(img)
    else:
        return 0
        
def main(input):
    Stuffschema = types.StructType([
    types.StructField('overall', types.DoubleType()),
    types.StructField('vote', types.StringType()),
    types.StructField('verified', types.BooleanType()),
    types.StructField('reviewTime', types.StringType()),
    types.StructField('reviewID', types.StringType()),
    types.StructField('asin', types.StringType()),
    types.StructField('style', types.StringType()),
    types.StructField('reviewerName', types.StringType()),
    types.StructField('reviewerID', types.StringType()),
    types.StructField('reviewText', types.StringType()),
    types.StructField('unixReviewTime', types.StringType()),
    types.StructField('image', types.StringType()),
    ]) 
    df = spark.read.json(input, schema = Stuffschema)
    df = df.fillna(value = 0.0, subset = ['overall'])
    df = df.fillna(value = '0', subset = ['vote'])
    weighted_Image = functions.udf(weightedImage, types.StringType()) 
    df = df.withColumn('Review_Weight',  functions.size(functions.split(df.reviewText, ' ')))
    df = df.withColumn('image', functions.from_json(df.image, types.ArrayType(types.StringType())))
    df = df.withColumn('Weighted_Image', weighted_Image(df.image))
    df2 = df.select(df.overall, df.Weighted_Image.cast('double'), df.vote.cast('double'), df.Review_Weight.cast('double'), df.asin, df.reviewerID, df.verified).filter(df.verified == 'true')
    df2 = df2.withColumn('Weight',(df2.Review_Weight + df2.Weighted_Image + df2.vote))
    weighted_Average = df2.groupBy(df2.asin).agg((functions.sum(df2.overall*df2.Weight)/functions.sum(df2.Weight)),functions.count('reviewerID'))
    weighted_Average = weighted_Average.toDF('Product_id', 'Weighted_Avg','Num_of_purchases')
    weighted_Average = weighted_Average.sort(weighted_Average.Num_of_purchases.desc())
    df2 = df2.join(weighted_Average, on = df2.asin == weighted_Average.Product_id)
    dfFinal = df2.select(df2.reviewerID, df2.asin, df2.Weighted_Avg, df2.Num_of_purchases) 

    metadata = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('title', types.StringType())
    ]) 

    path = '/Users/jarvis/Amazon_Product_Analysis/Dataset/meta_Clothing_Shoes_and_Jewelry.json.gz'

    metaDf = spark.read.json(path, schema = metadata)
    metaDf = metaDf.toDF('prod_id','product_name')
    resultDF = dfFinal.join(metaDf, on = dfFinal.asin == metaDf.prod_id)
    output = resultDF.distinct()

    #Top products with highest weighted_avg
    res = output.groupBy(output.product_name).agg(functions.max('Weighted_Avg').alias('final_weighted_avg'))
    pandDF = res.limit(1000).toPandas()
    WeightedDf = pandDF.sort_values(by = ['final_weighted_avg'], ascending = False)
    WeightedDf.to_csv("/Users/jarvis/Amazon_Product_Analysis/Results/weighted_df.csv")
    
    #which customer prefers which product in this category,
    #num of purchase and the product name:
    dfPur = output.groupBy(output.product_name).agg(functions.max('Num_of_purchases').alias('Num_purchases'))
    pandDf1 = dfPur.limit(100).toPandas()
    sortedDfPur = pandDf1.sort_values(by = ['Num_purchases'], ascending = False)
    sortedDfPur.to_csv("/Users/jarvis/Amazon_Product_Analysis/Results/clothing.csv")
    


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Parse Json file').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = "/Users/jarvis/Amazon_Product_Analysis/Dataset/Clothing_Shoes_and_Jewelry_5.json.gz"
    main(inputs)
