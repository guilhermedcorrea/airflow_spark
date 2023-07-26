from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.pyspark_hook import PySparkHook

class PySparkOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            pyspark_script,
            pyspark_conn_id='pyspark_default',
            *args, **kwargs):
        super(PySparkOperator, self).__init__(*args, **kwargs)
        self.pyspark_script = pyspark_script
        self.pyspark_conn_id = pyspark_conn_id

    def execute(self, context):
        hook = PySparkHook(conn_id=self.pyspark_conn_id)
        spark = hook.get_spark()

        try:
            exec(self.pyspark_script)
        finally:
            spark.stop()
