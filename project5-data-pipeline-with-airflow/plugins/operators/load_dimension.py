from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='', 
                 table='',
                 sql_query='',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.truncate:
            redshift.run(f'TRUNCATE TABLE {self.table}')
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table, 
            self.sql_query)
        redshift.run(formatted_sql)
        
        self.log.info(f"Success: Creating {self.table} in Redshift")