import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step trainer Trusted
SteptrainerTrusted_node1724250413295 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="step_trainer_trusted", transformation_ctx="SteptrainerTrusted_node1724250413295")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724250512532 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724250512532")

# Script generated for node Join
Join_node1724250549333 = Join.apply(frame1=SteptrainerTrusted_node1724250413295, frame2=AccelerometerTrusted_node1724250512532, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1724250549333")

# Script generated for node Amazon S3
AmazonS3_node1724250602607 = glueContext.getSink(path="s3://hieulm-lake-house/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724250602607")
AmazonS3_node1724250602607.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="machine_learning_curated")
AmazonS3_node1724250602607.setFormat("json")
AmazonS3_node1724250602607.writeFrame(Join_node1724250549333)
job.commit()