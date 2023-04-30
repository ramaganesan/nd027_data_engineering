from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    This operator Loads data into Redshift Dimensions table
    :parameter
    redshift_conn_id: str
      Reshift Connection Details
    sql: str
      Sql query to provide data to load
    table: str
      Redshift table to load data
    truncate : bool
      Truncate table
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 table='',
                 truncate=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        """
        Executes Dimension table load
        :param context:
        :return:
        """
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load dimension table {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')

        self.log.info(f'Dimension Redshift table {self.table} loaded ')
