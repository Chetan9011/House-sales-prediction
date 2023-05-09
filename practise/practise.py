import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"],)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df=spark.read.csv("s3://input0204/members.csv",header=True,inferSchema=True)
# Two columns concantated

#df2=df.withColumn("new2",df.source)

df.write.format("csv").mode("overwrite").option("header",True).save("s3://op08/1/data/")




