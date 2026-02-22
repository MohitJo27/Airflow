from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_kwargs_dag"
)
def xcoms_kwargs_dag():
    
    @task.python
    def first_task(**kwargs):

        # Extracting 'ti' from kwargs to push XComs manually
        ti = kwargs['ti']

        print("Extracting the data! This is the first task")
        featched_data = {'data': [1, 2, 3, 4, 5]}
        # Pushing XComs manually
        ti.xcom_push(key= 'return_result', value= featched_data)


    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        # Pulling XComs pushed by the first task
        featched_data = ti.xcom_pull(task_ids="first_task", key = "return_result")['data']
        transformed_data = featched_data * 2
        transformed_data_dict = {'transformed_data': transformed_data}
        # Pushing XComs manually
        ti.xcom_push(key= 'return_result', value= transformed_data_dict)
        

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        # Pulling XComs pushed by the second task
        loaded_data = ti.xcom_pull(task_ids="second_task", key = "return_result")['transformed_data']
        return loaded_data

    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    first = first_task()
    second = second_task() 
    third = third_task()

    first >> second >> third

# Instantiate the DAG
xcoms_kwargs_dag()