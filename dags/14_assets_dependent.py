from airflow.sdk import dag, task, asset
from pendulum import datetime
from assets_13 import fetch_data
import os

@asset(
    schedule=fetch_data,
    # This is optional but to good to include for clarity asset's location
    uri="/opt/airflow/logs/data/data_processed.txt",
    name="process_data"
)
def process_data(self):
    """Process the extracted data"""
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    # Simulate data fetching by writing to a file
    with open(self.uri, "w") as f:
        f.write(f"Data processed successfully!!")

    print(f"Data processed to {self.uri}")