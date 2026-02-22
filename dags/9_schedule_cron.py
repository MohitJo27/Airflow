from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="cron_schedule_dag",
    start_date=datetime(year= 2026, month=2, day=1, tz="Asia/Kolkata"),
    schedule= CronTriggerTimetable("0 16 * * MON-FRI", timezone= "Asia/Kolkata"),
    end_date=datetime(year= 2026, month=2, day=28, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True,
    tags=["cron_schedule_dag", "cron"]
)
def cron_schedule_dag():
    
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
cron_schedule_dag()