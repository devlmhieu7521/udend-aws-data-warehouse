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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724077809279 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724077809279")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724077826627 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724077826627")

# Script generated for node Join
Join_node1724077834444 = Join.apply(frame1=AWSGlueDataCatalog_node1724077809279, frame2=AWSGlueDataCatalog_node1724077826627, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724077834444")

# Script generated for node Filter Customer Curated
SqlQuery0 = '''
select
    distinct
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
from myDataSource
'''
FilterCustomerCurated_node1724077853560 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724077834444}, transformation_ctx = "FilterCustomerCurated_node1724077853560")

# Script generated for node Customer Curated
CustomerCurated_node1724078033687 = glueContext.getSink(path="s3://hieulm-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1724078033687")
CustomerCurated_node1724078033687.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="customer_curated")
CustomerCurated_node1724078033687.setFormat("json")
CustomerCurated_node1724078033687.writeFrame(FilterCustomerCurated_node1724077853560)
job.commit()