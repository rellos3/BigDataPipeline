from __future__ import annotations
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The big_data_python_demo DAG requires virtualenv, please install it.")
else:
    @dag(schedule=None, 
         start_date=datetime(2021, 1, 1), 
         catchup=False, 
         tags=["big_data"])
    def home_selection_pipeline():
        """
        ### Data Pipeline for Home Selection

        This DAG demonstrates a data pipeline for selecting the best affordable home.
        """

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["funcsigs"],
        )
        def Collect():
            """
            #### Collect Task

            A task to collect and prepare the dataset for analysis.
            Simulates data collection (e.g., from a JSON string).
            """
            import json

            # Simulated data collection (replace with actual data retrieval)
            data_string = '{"ID": [1, 2, 3, 4, 5], "Location": ["Suburban", "Urban", "Rural", "Suburban", "Urban"], "Price": [250000, 180000, 150000, 300000, 200000], "Income": [80000, 60000, 50000, 100000, 70000]}'
            dataset = json.loads(data_string)
            return dataset

        @task()
        def Analyze(dataset: dict):
            """
            #### Analyze Task

            A task to analyze the collected dataset and determine key factors.
            """
            import pandas as pd

            # Convert dataset to DataFrame for analysis
            df = pd.DataFrame(dataset)

            # Perform analysis (e.g., calculate mean, median, etc.)
            mean_price = df['Price'].mean()
            median_income = df['Income'].median()

            return {
                "mean_price": mean_price,
                "median_income": median_income
            }

        @task()
        def Organize(dataset: dict):
            """
            #### Organize Task

            A task to organize and preprocess the data for further processing.
            """
            # Perform data organization and preprocessing here
            organized_data = dataset  # Placeholder for data organization
            return organized_data

        @task()
        def Predict(dataset: dict):
            """
            #### Predict Task

            A task to make predictions based on the analyzed and organized data.
            """
            # Placeholder for prediction task (e.g., machine learning model)
            prediction = "Placeholder prediction"
            return prediction

        # Define task dependencies
        collected_data = Collect()
        analysis_results = Analyze(collected_data)
        organized_data = Organize(collected_data)
        prediction_result = Predict(organized_data)

    home_selection_dag = home_selection_pipeline()
