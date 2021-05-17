from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json


class PutToRedshiftOperator(BaseOperator):
    sql_query = "INSERT INTO CENTERS(day,month,year,center_id,name,pincode,district_name,vaccine,min_age_limit,fee) " \
                "VALUES({day},{month},{year},{center_id},'{name}',{pincode},'{district_name}','{vaccine}',{min_age_limit},{fee})"
    
    query = "INSERT INTO DATA " \
        	"VALUES({day},{month},{year},{positive},{tests},{recovered},{deaths},{vaccinated},{first_dose},{second_dose},{active_cases},{zones})"


    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        file_name="",
        OP_name="",
        *args, **kwargs):
            super(PutToRedshiftOperator, self).__init__(*args, **kwargs)
            self.redshift_conn_id = redshift_conn_id
            self.file_name = file_name
            self.OP_name = OP_name


    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        data = json.load(open(self.file_name))
        if self.OP_name == "vac":
            for vaues in data:
                str = PutToRedshiftOperator.sql_query.format(**vaues)
                print(str)
                redshift.run(str)

        if self.OP_name == "data":
            str = PutToRedshiftOperator.query.format(**data)
            print(str)
            redshift.run(str)