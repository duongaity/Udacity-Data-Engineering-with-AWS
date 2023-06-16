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

# Script generated for node Trainer Trusted
TrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-using-spark-in-aws/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="TrainerTrusted_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1686905420454 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-using-spark-in-aws/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1686905420454",
)

# Script generated for node Join
Join_node1686905455892 = Join.apply(
    frame1=TrainerTrusted_node1,
    frame2=AmazonS3_node1686905420454,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1686905455892",
)

# Script generated for node Drop Fields
DropFields_node1686905480166 = DropFields.apply(
    frame=Join_node1686905455892,
    paths=["timeStamp"],
    transformation_ctx="DropFields_node1686905480166",
)

# Script generated for node Trainer Curated
TrainerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686905480166,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-using-spark-in-aws/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="TrainerCurated_node3",
)

job.commit()
