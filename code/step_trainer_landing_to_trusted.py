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
CustomerCurated_node1724249888495 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724249888495")

# Script generated for node Step trainer Landing
SteptrainerLanding_node1724249903511 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="step_trainer_landing", transformation_ctx="SteptrainerLanding_node1724249903511")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1724250120248 = ApplyMapping.apply(frame=SteptrainerLanding_node1724249903511, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1724250120248")

# Script generated for node Join
Join_node1724249918815 = Join.apply(frame1=CustomerCurated_node1724249888495, frame2=RenamedkeysforJoin_node1724250120248, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1724249918815")

# Script generated for node SQL Query
SqlQuery0 = '''
select
    distinct
    sensorreadingtime,
    serialnumber,
    distancefromobject
from myDataSource
'''
SQLQuery_node1724249937571 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724249918815}, transformation_ctx = "SQLQuery_node1724249937571")

# Script generated for node Amazon S3
AmazonS3_node1724250197546 = glueContext.getSink(path="s3://hieulm-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724250197546")
AmazonS3_node1724250197546.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="step_trainer_trusted")
AmazonS3_node1724250197546.setFormat("json")
AmazonS3_node1724250197546.writeFrame(SQLQuery_node1724249937571)
job.commit()