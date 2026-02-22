from dag_orchestrate_1 import first_orchestrator_dag
from dag_orchestrate_2 import second_orchestrator_dag
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def dag_orchestrate_parent():

    first_orchestrator_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestrator_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True # Optional (This is slow): Waits for the triggered DAG to complete
    )

    second_orchestrator_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestrator_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True # Optional (This is slow): Waits for the triggered DAG to complete
    )

    first_orchestrator_dag >> second_orchestrator_dag

# Instantiating the DAG
dag_orchestrate_parent()
