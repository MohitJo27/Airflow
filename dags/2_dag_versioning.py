from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag"
)
def versioned_dag():
    
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")\


    @task.python
    def version_task():
        print("This is the version task. DAG Versioning 2")

    @task.python
    def fourth_task():
        print("This is the fourth task")

    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    first = first_task()
    second = second_task()
    third = third_task()
    version= version_task()
    fourth = fourth_task()

    first >> second >> third >> version >> fourth

# Instantiate the DAG
versioned_dag()