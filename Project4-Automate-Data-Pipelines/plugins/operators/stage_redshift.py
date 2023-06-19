"""Loads data from S3 to Redshift."""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    copy_sql_cmd = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        compupdate off
        {}
        ;
    """
  
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table_name = "",
                 json_paths = "",
                 s3_bucket = "",
                 s3_key = "",
                 aws_region = "",
                 extra_params = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region
        self.extra_params = extra_params


    def execute(self, context):
        """
        Extract and Load data from JSON on S3 into staging tables on Redshift.
        """

        # Config Env
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Set URL S3 Path
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info("StageToRedshiftOperator - S3 Path: {s3_path}")

        # Run cmd
        formatted_sql_cmd = StageToRedshiftOperator.copy_sql_cmd.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.aws_region,
            self.extra_params,
        )
        redshift.run(formatted_sql_cmd)

        self.log.info("StageToRedshiftOperator - Loads data from S3 to Redshift done")
