from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.postgres_hook import PostgresHook

class PostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            sql,
            postgres_conn_id='postgres_default',
            *args, **kwargs):
        super(PostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(self.sql)
