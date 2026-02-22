from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_dag_data"
)
def xcoms_dag_data():
    
    @task.python
    def first_task():
        print("Extracting the data! This is the first task")
        featched_data = {'data': [1, 2, 3, 4, 5]}
        return featched_data

    @task.python
    def second_task(data: dict):
        featched_data = data['data']
        transformed_data = featched_data * 2
        transformed_data_dict = {'transformed_data': transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data: dict):
        loaded_data = data
        print(f"This is the third task of Loading the data {loaded_data}")

    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    first = first_task()
    second = second_task(first) # what ever value return by the first task will be passed as an argument to the second task
    third = third_task(second) # what ever value return by the second task will be passed as an argument to the third task

    first >> second >> third

# Instantiate the DAG
xcoms_dag_data()