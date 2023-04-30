from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        This operator Loads data into Redshift Facts table
        :parameter
        redshift_conn_id: str
          Reshift Connection Details
        facts_table_name: str
          Redshift fact table to check
        tables_to_check: list
          Redshift Dimension tables to check
        """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 facts_table_name="songplays",
                 tables_to_check=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.facts_table_name = facts_table_name
        self.tables_to_check = tables_to_check

    def execute(self, context):
        """
        Data Quality check for Redshift tables
        :param context:
        :return:
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.table_data_check(redshift_hook, self.facts_table_name, True)

        if self.tables_to_check:
            for table in self.tables_to_check:
                self.table_data_check(redshift_hook, table, False)

    def table_data_check(self, redshift_hook, table_name, raise_error):
        """
        This util function does data quality check on table
        Checks if the table data is not empty
        Checks if the table data has valid rows
        :param redshift_hook:
        :param table_name:
        :param raise_error: If this value is True will raise error and stop execution of code
        :return:
        """
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
        if len(records) < 1 or len(records[0]) < 1:
            self.log.info(f'Data quality check failed. {table_name} returned no results')
            if raise_error:
                raise ValueError(f'Data quality check failed. {table_name} returned no results')
        num_records = records[0][0]
        if num_records < 1:
            self.log.info(f'Data quality check failed. {table_name} contained 0 rows')
            if raise_error:
                raise ValueError(f'Data quality check failed. {table_name} contained 0 rows')
        self.log.info(f'Data quality on table {table_name} check passed with {records[0][0]} records')

