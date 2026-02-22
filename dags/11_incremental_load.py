from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id= "Incremental_load_dag",
    start_date= datetime(year= 2026, month=1, day=26, tz="Asia/Kolkata"),
    schedule= CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
    end_date= datetime(year= 2026, month=2, day=28, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup= True, #backfill
    tags= ["incremental_load_dag", "incremental"]
)
def Incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):

        date_interval_start = kwargs['data_interval_start'] # data_interval_start is by default provided by airflow
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data for {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process(**kwargs):
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"

    incremental_data_fetch() >> incremental_data_process()

# Instantiating the DAG
Incremental_load_dag()