from airflow.sdk import dag, task

@dag(
    dag_id="parallel_dag"
)
def parallel_dag():
    
    @task.python
    def extract_task(**kwargs):
        print("Extracting Data........")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_data": [1,2,3,4,5],
            "db_data": [6,7,8,9,10],
            "s3_extracted_data": [11,12,13,14,15]
        }
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_task')['api_data']
        print("Transforming API Data: {api_extracted_data}.......")
        transformed_api_data = [i+1 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti=kwargs['ti']
        db_extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_task')['db_data']
        print("Transforming DB Data: {db_extracted_data}.......")
        transformed_db_data = [i+1 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti=kwargs['ti']
        s3_extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_task')['s3_extracted_data']
        print("Transforming S3 Data: {s3_extracted_data}.......")
        transformed_s3_data = [i+1 for i in s3_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)

    # @task.python
    # def merge_task(**kwargs):
    #     ti = kwargs['ti']
    #     api= ti.xcom_pull(key='return_value', task_ids='transform_task_api')
    #     db= ti.xcom_pull(key='return_value', task_ids='transform_task_db')
    #     s3= ti.xcom_pull(key='return_value', task_ids='transform_task_s3')
    #     print("Merging Data: {api}, {db}, {s3}.......")
    @task.bash
    def load_task(**kwargs):
        ti = kwargs['ti']
        api= ti.xcom_pull(key='return_value', task_ids='transform_task_api')
        db= ti.xcom_pull(key='return_value', task_ids='transform_task_db')
        s3= ti.xcom_pull(key='return_value', task_ids='transform_task_s3')
        return f"echo 'Loaded Data: {api}, {db}, {s3}.......'"
    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    extract_task = extract_task()
    transform_task_api = transform_task_api()
    transform_task_db = transform_task_db()
    transform_task_s3 = transform_task_s3()
    load_task = load_task()

    extract_task >> [transform_task_api, transform_task_db, transform_task_s3] >> load_task

# Instantiate the DAG
parallel_dag()