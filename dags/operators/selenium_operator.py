from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.selenium_hook import SeleniumHook

class SeleniumOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            selenium_script,
            selenium_conn_id='selenium_default',
            *args, **kwargs):
        super(SeleniumOperator, self).__init__(*args, **kwargs)
        self.selenium_script = selenium_script
        self.selenium_conn_id = selenium_conn_id

    def execute(self, context):
        hook = SeleniumHook(conn_id=self.selenium_conn_id)
        driver = hook.get_driver()

        try:
            exec(self.selenium_script)
        finally:
            driver.quit()
