# Imports
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.providers.standard.operators.hitl import HITLEntryOperator

from pendulum import datetime

# Dag definition
@dag(
  start_date=datetime(2026,5,1),
  schedule='@monthly',
  on_success_callback=SmtpNotifier(
    to="airflow_team@mycompany.com",
    from_email="airflow_production@mycompany.com",
    subject="sales_etl run succeeded!"
  )
)
def sales_etl():
  
  @task
  def initialize(ds):
    print('Cleaning data directories, prepping for processing...')

    # Use a variable to set the current data path
    Variable.set("sales_etl_data_path", f'/data/sales_etl/{ds}')
  
  
  # Setup a file sensor to wait for the filescopied.txt file to be present
  filewatcher = FileSensor(
    task_id="WaitForDataFiles",
    filepath="{{var.value.sales_etl_data_path}}/filescopied.txt",
    poke_interval=15,
    timeout=300
  )
  
  
  # Create a branching task to validate the data. 
  @task.branch
  def validate_data():
    sales_path = Variable.get('sales_etl_data_path')
    print(f"Parsing data at {sales_path}")
    next_task = 'regular_monthly_task'
    if '-09-' in sales_path:
      next_task = 'yearend_approval_task'
    return next_task
  
  @task
  def regular_monthly_task():
    print(f'Processing data and automatically updating dataset')
    
  yearend_approval_task = HITLEntryOperator(
    task_id="____",
    subject="Sales data processing - Approval Required",
    body=(
            "Please review the sales data produced by the *validate_data* task "
            "on {{ds}}."
            "Approve to push to the data warehouse, or Reject to halt the run."
    ),
  )
  
  @task
  def push_to_warehouse():
    print(f'Pushing processed data to warehouse')
  
  initialized = initialize()
  validated = validate_data()
  
  initialized >> filewatcher >>validated
  
  validated >> [regular_monthly_task(), ____] >> push_to_warehouse()
  
  
sales_etl()
