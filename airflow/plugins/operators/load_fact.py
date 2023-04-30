from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator Loads data into Redshift Facts table
    :parameter
    redshift_conn_id: str
      Reshift Connection Details
    table: str
      Redshift table to load data
    table_values: str
      Sql Query to Load data
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_values="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_values = table_values

    def execute(self, context):
        """
        Loads data into Redshift table
        :param context:
        :return:
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate table
        self.log.info(f'Truncate table {self.table}')
        redshift.run(f'TRUNCATE {self.table}')

        # Execute the insertion query on Redshift hook
        redshift.run(f'INSERT INTO {self.table} {self.table_values}')
        self.log.info(f'Data inserted into Redshift table {self.table}')
