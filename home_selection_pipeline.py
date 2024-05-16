from __future__ import annotations
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The home_selection_pipeline DAG requires virtualenv, please install it.")
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
            requirements=["funcsigs", "matplotlib", "pandas"],
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
                "median_income": median_income,
                "dataset": dataset  # Pass the dataset along for further processing
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

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["matplotlib", "pandas"],
        )
        def Visualize1(dataset: dict):
            """
            #### Visualization Task 1

            A task to generate and save a bar chart of home prices.
            """
            import pandas as pd
            import matplotlib.pyplot as plt

            df = pd.DataFrame(dataset)
            df.plot(kind='bar', x='ID', y='Price')
            plt.title('Home Prices')
            plt.xlabel('Home ID')
            plt.ylabel('Price')
            plt.savefig('C:/airflow/output/home_prices.png')
            plt.close()

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["matplotlib", "pandas"],
        )
        def Visualize2(dataset: dict):
            """
            #### Visualization Task 2

            A task to generate and save a histogram of income.
            """
            import pandas as pd
            import matplotlib.pyplot as plt

            df = pd.DataFrame(dataset)
            df['Income'].plot(kind='hist')
            plt.title('Income Distribution')
            plt.xlabel('Income')
            plt.ylabel('Frequency')
            plt.savefig('C:/airflow/output/income_distribution.png')
            plt.close()

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["matplotlib", "pandas"],
        )
        def Visualize3(dataset: dict):
            """
            #### Visualization Task 3

            A task to generate and save a scatter plot of home prices vs income.
            """
            import pandas as pd
            import matplotlib.pyplot as plt

            df = pd.DataFrame(dataset)
            df.plot(kind='scatter', x='Income', y='Price')
            plt.title('Home Prices vs Income')
            plt.xlabel('Income')
            plt.ylabel('Price')
            plt.savefig('C:/airflow/output/prices_vs_income.png')
            plt.close()

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["matplotlib", "pandas"],
        )
        def Visualize4(dataset: dict):
            """
            #### Visualization Task 4

            A task to generate and save a pie chart of home locations.
            """
            import pandas as pd
            import matplotlib.pyplot as plt

            df = pd.DataFrame(dataset)
            location_counts = df['Location'].value_counts()
            location_counts.plot(kind='pie', autopct='%1.1f%%')
            plt.title('Home Locations')
            plt.ylabel('')
            plt.savefig('C:/airflow/output/home_locations.png')
            plt.close()

        # Define task dependencies
        collected_data = Collect()
        analysis_results = Analyze(collected_data)
        organized_data = Organize(collected_data)
        prediction_result = Predict(organized_data)
        visualize1_result = Visualize1(collected_data)
        visualize2_result = Visualize2(collected_data)
        visualize3_result = Visualize3(collected_data)
        visualize4_result = Visualize4(collected_data)

    home_selection_dag = home_selection_pipeline()
