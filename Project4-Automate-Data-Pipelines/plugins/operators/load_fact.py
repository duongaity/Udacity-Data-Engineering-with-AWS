"""Load Fact Tables."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql_cmd = """
        INSERT INTO {} {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 database_name = "",
                 table_name = "",
                 table_fields = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database_name = database_name
        self.table_name = table_name
        self.table_fields = table_fields
        self.sql_query = sql_query

    def execute(self, context):
        # Config Env
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("LoadFactOperator - Loading fact table '{self.table_name}' into Redshift")
        self.log.info("LoadFactOperator - Running SQL query: {self.sql_query}")

        # Run cmd
        formatted_sql_cmd = LoadFactOperator.insert_sql_cmd.format(
            self.table_name,
            self.table_fields,
            self.sql_query
        )
        redshift.run(str(formatted_sql_cmd))

        self.log.info("LoadFactOperator - Loading fact table '{self.table_name}' done")
