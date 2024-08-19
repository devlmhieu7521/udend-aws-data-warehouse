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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1723993612965 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1723993612965")

# Script generated for node Customer Landing
CustomerLanding_node1723993592216 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_hieulm", table_name="customer_landing", transformation_ctx="CustomerLanding_node1723993592216")

# Script generated for node Join
Join_node1723993808519 = Join.apply(frame1=AccelerometerLanding_node1723993612965, frame2=CustomerLanding_node1723993592216, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1723993808519")

# Script generated for node Drop Fields
DropFields_node1723994497599 = DropFields.apply(frame=Join_node1723993808519, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1723994497599")

# Script generated for node Amazon S3
AmazonS3_node1723993929869 = glueContext.getSink(path="s3://hieulm-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723993929869")
AmazonS3_node1723993929869.setCatalogInfo(catalogDatabase="stedi_project_hieulm",catalogTableName="accelerometer_trusted")
AmazonS3_node1723993929869.setFormat("json")
AmazonS3_node1723993929869.writeFrame(DropFields_node1723994497599)
job.commit()