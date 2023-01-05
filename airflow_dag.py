import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor



# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'Alex_Hill',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://wcdmidtermscriptsajh/miderm_pyspark.py',
                '--calpath', "{{ task_instance.xcom_pull('parse_request', key='calendar') }}",
                '--invpath', "{{ task_instance.xcom_pull('parse_request', key='inventory') }}",
                '--prodpath', "{{ task_instance.xcom_pull('parse_request', key='product') }}",
                '--salespath', "{{ task_instance.xcom_pull('parse_request', key='sales') }}",
                '--storepath', "{{ task_instance.xcom_pull('parse_request', key='store') }}",
                '--output_bucket', 's3://wcdmidtermoutputajh/output/',
                # '-c', 'job',
                # '-m', 'append',
                # '--input-options', 'header=true'
            ]
        }
    }

]


CLUSTER_ID = "j-3VJ6EEQKBEV8H"

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2022,9,22),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):
    calendar = kwargs['dag_run'].conf['calendar']
    kwargs['ti'].xcom_push(key = 'calendar', value = calendar)

    inventory = kwargs['dag_run'].conf['inventory']
    kwargs['ti'].xcom_push(key = 'inventory', value = inventory)

    product = kwargs['dag_run'].conf['product']
    kwargs['ti'].xcom_push(key = 'product', value = product)

    sales = kwargs['dag_run'].conf['sales']
    kwargs['ti'].xcom_push(key = 'sales', value = sales)

    store = kwargs['dag_run'].conf['store']
    kwargs['ti'].xcom_push(key = 'store', value = store)


dag=DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=1),
    start_date=datetime(2022,9,22),
    schedule_interval = None,
    description='WCD Midterm DAG',
    catchup=False,
) 

t0 = DummyOperator(
    task_id='dag_start'
)

parse_request = PythonOperator(task_id = 'parse_request',
                            provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                            python_callable = retrieve_s3_files,
                            dag = dag
) 

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)

t0>>parse_request>>step_adder>>step_checker 
    #step_adder.set_upstream(parse_request)
    #step_checker.set_upstream(step_adder)

