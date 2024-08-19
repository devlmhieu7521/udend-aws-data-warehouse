import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1724075382481 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724075382481")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1724075435864 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724075435864")

# Script generated for node Join
Join_node1724075479067 = Join.apply(frame1=CustomerTrusted_node1724075382481, frame2=AccelerometerLanding_node1724075435864, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724075479067")

# Script generated for node SQL Query
SqlQuery0 = '''
select user, timestamp, x, y, z from myDataSource
'''
SQLQuery_node1723993392480 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724075479067}, transformation_ctx = "SQLQuery_node1723993392480")

# Script generated for node Amazon S3
AmazonS3_node1723993929869 = glueContext.getSink(path="s3://hieulm-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723993929869")
AmazonS3_node1723993929869.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="accelerometer_trusted")
AmazonS3_node1723993929869.setFormat("json")
AmazonS3_node1723993929869.writeFrame(SQLQuery_node1723993392480)
job.commit()