from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    This operator loads data from S3 into Redshift Cluster

    :parameter
        redshift_conn_id: str
          Reshift Connection Details
        aws_credentials_id: str
          AWS Credentials
        table: str
          Redshift table to load data
        s3_bucket: str
          S3 Bucket Details
        s3_key: str
          S3 Key
        data_format: str
          Data format suggestion to load S3 data
        skip_staging: bool
            Used to skip this step for Re runs
    """
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
        """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 data_format="",
                 skip_staging=False,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.data_format = data_format
        self.skip_staging = skip_staging

    def execute(self, context):
        """
        Loads data from S3 bucket to Redshift Cluster
        :param context:
        :return:
        """
        if self.skip_staging:
            self.log.info(f'data already loaded for table {self.table}, skipping load')
        else:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            ## Clearing data from Redshift table
            try:
                self.log.info(f'Clearing data from destination Redshift table if present {self.table}')
                redshift.run("DELETE FROM {}".format(self.table))
            except:
                self.log.info(f'destination Redshift table {self.table} continuing...')

            ## Copying data from S3 into Reshift
            self.log.info("Copying data from S3 to Redshift")
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.data_format
            )
            redshift.run(formatted_sql)
            self.log.info(f'data from S3 loaded into Redshift table {self.table}')
