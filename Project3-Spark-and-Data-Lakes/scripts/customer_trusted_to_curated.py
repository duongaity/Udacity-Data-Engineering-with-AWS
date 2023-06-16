import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customers Trusted
CustomersTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-using-spark-in-aws/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomersTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1686903659030 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-using-spark-in-aws/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1686903659030",
)

# Script generated for node Join
Join_node1686903702433 = Join.apply(
    frame1=CustomersTrusted_node1,
    frame2=AccelerometerLanding_node1686903659030,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686903702433",
)

# Script generated for node Drop Fields
DropFields_node1686903744828 = DropFields.apply(
    frame=Join_node1686903702433,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1686903744828",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686903744828,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-using-spark-in-aws/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
