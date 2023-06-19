"""Load Dimension Tables."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql_cmd = """
        INSERT INTO {} {}
        {};
    """
    
    truncate_sql_cmd = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 database_name = "",
                 table_name = "",
                 table_fields = "",
                 table_truncate = False,
                 sql_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database_name = database_name
        self.table_name = table_name
        self.table_fields = table_fields
        self.table_truncate = table_truncate
        self.sql_query = sql_query

    def execute(self, context):
        # Config Env
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.table_truncate:
            self.log.info(f"TRUNCATE TABLE: {self.table_name}")
            redshift.run(f"TRUNCATE TABLE {self.table_name}")

        self.log.info("LoadDimensionOperator - Loading dimension table '{self.table_name}'")
        
        # Run cmd
        formatted_sql_cmd = LoadDimensionOperator.insert_sql_cmd.format(
            self.table_name,
            self.table_fields,
            self.sql_query
        )
        redshift.run(formatted_sql_cmd)

        self.log.info("LoadDimensionOperator - Loading dimension table '{self.table_name}' done")
