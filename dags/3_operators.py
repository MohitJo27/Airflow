from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="operators_dag"
)
def operators_dag():
    
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def run_after_loop() -> str:
        return "echo https://airflow.apache.org/"

    run_this2= BashOperator(
        task_id="run_this_bash",
        bash_command="echo https://airflow.apache.org/"
    )

    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    first = first_task()
    second = second_task()
    run_this = run_after_loop()

    first >> second >> run_this >> run_this2

# Instantiate the DAG
operators_dag()
