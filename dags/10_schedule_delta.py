from airflow.sdk import dag, task
from pendulum import datetime, Duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="delta_schedule_dag",
    start_date=datetime(year= 2026, month=2, day=1, tz="Asia/Kolkata"),
    schedule= DeltaTriggerTimetable(Duration(days=3)),
    end_date=datetime(year= 2026, month=2, day=28, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True,
    tags=["delta_schedule_dag", "delta"]
)
def delta_schedule_dag():
    
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    # Define the order of tasks
    # first_task() >> second_task() >> third_task() or

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

# Instantiate the DAG
delta_schedule_dag()