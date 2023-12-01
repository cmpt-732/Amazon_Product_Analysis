from pyspark.sql import SparkSession, functions, types
import sys
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
    #df.show(100)
    df = df.fillna(value = 0.0, subset = ['overall'])
    df = df.fillna(value = '0', subset = ['vote'])
    weighted_Image = functions.udf(weightedImage, types.StringType()) 
    df = df.withColumn('Review_Weight',  functions.size(functions.split(df.reviewText, ' ')))
    df = df.withColumn('image', functions.from_json(df.image, types.ArrayType(types.StringType())))
    df = df.withColumn('Weighted_Image', weighted_Image(df.image))
    df2 = df.select(df.overall, df.Weighted_Image.cast('double'), df.vote.cast('double'), df.Review_Weight.cast('double'), df.asin, df.reviewerID, df.verified).filter(df.verified == 'true')
    df2 = df2.withColumn('Weight',(df2.Review_Weight + df2.Weighted_Image + df2.vote))
    #df2.show(100)
    #print(df2.dtypes)
    weighted_Average = df2.groupBy(df2.asin).agg((functions.sum(df2.overall*df2.Weight)/functions.sum(df2.Weight)),functions.count('reviewerID'))
    #weighted_Average.show(10)
    weighted_Average = weighted_Average.toDF('Product_id', 'Weighted_Avg','Num_of_purchases')
    #weighted_Average.show(10) 
    df2 = df2.join(weighted_Average, on = df2.asin == weighted_Average.Product_id)
    dfFinal = df2.select(df2.reviewerID, df2.asin, df2.Weighted_Avg, df2.Num_of_purchases) 
    #dfFinal.show(10)

    metadata = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('title', types.StringType())
    ]) 

    path = '/Users/hersh/Documents/BigDataLab/Project/meta_Clothing_Shoes_and_Jewelry.json'

    metaDf = spark.read.json(path, schema = metadata)
    metaDf = metaDf.toDF('prod_id','product_name')
    resultDF = dfFinal.join(metaDf, on = dfFinal.asin == metaDf.prod_id)
    #resultDF.show(50)
    output = resultDF.distinct()
    #output.show(50)
    res = output.select(output.reviewerID, output.asin, output.Weighted_Avg, output.Num_of_purchases, output.product_name)
    res.show(10)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Parse Json file').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    main(inputs)
