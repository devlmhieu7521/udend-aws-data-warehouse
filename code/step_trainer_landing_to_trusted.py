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

# Script generated for node Customer Curated
CustomerCurated_node1724078966217 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724078966217")

# Script generated for node Step trainer landing
Steptrainerlanding_node1724078984447 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="step_trainer_landing", transformation_ctx="Steptrainerlanding_node1724078984447")

# Script generated for node Join
Join_node1724079045762 = Join.apply(frame1=Steptrainerlanding_node1724078984447, frame2=CustomerCurated_node1724078966217, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1724079045762")

# Script generated for node Filter Step trainer
SqlQuery0 = '''
select
    distinct
    sensorreadingtime,
    serialnumber,
    distancefromobject
from myDataSource
'''
FilterSteptrainer_node1724079126180 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724079045762}, transformation_ctx = "FilterSteptrainer_node1724079126180")

# Script generated for node Amazon S3
AmazonS3_node1724079177349 = glueContext.getSink(path="s3://hieulm-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724079177349")
AmazonS3_node1724079177349.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="step_trainer_trusted")
AmazonS3_node1724079177349.setFormat("json")
AmazonS3_node1724079177349.writeFrame(FilterSteptrainer_node1724079126180)
job.commit()