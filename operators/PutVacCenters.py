from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json


class PutCentersOperator(BaseOperator):
    sql_query = "INSERT INTO CENTERS(day,month,year,center_id,name,pincode,district_name,vaccine,min_age_limit,fee) " \
                "VALUES({day},{month},{year},{center_id},'{name}',{pincode},'{district_name}','{vaccine}',{min_age_limit},{fee})"


    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        file_name="",
        *args, **kwargs):
        super(PutCentersOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.file_name = file_name


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        data = json.load(open(self.file_name))
        for vaues in data:
            str = PutCentersOperator.sql_query.format(**vaues)
            print(str)
            redshift.run(str)