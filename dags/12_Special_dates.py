from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable([
    datetime(2026, 1, 1),
    datetime(2026, 1, 17),
    datetime(2026, 1, 30),
    datetime(2026, 2, 5),
])

@dag(
    dag_id="Special_dates_dag",
    schedule=special_dates,
    start_date=datetime(year=2026, month=1, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=2, day=28, tz="Asia/Kolkata"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["special_dates_dag", "special_dates"]
)
def Special_dates_dag():
    
    @task.python
    def Special_dates_task(**kwargs):
        # logical_date is by default provided by airflow and its is the date on which the task is executed. It provide all the date for this execution
        execution_date = kwargs['logical_date'] 
        print(f"Special dates task executed on {execution_date}")

    Special_dates_task()

# Instantiating the DAG
Special_dates_dag()
