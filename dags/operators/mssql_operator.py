from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.mssql_hook import MsSqlHook

class MsSqlOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            sql,
            mssql_conn_id='mssql_default',
            *args, **kwargs):
        super(MsSqlOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mssql_conn_id = mssql_conn_id

    def execute(self, context):
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        hook.run(self.sql)
