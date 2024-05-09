from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

# Sample dataset representing potential home attributes
data = {
    "ID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "Location": ["Suburban", "Urban", "Rural", "Suburban", "Urban", "Rural", "Suburban", "Urban", "Rural", "Suburban"],
    "Price": [250000, 180000, 150000, 300000, 200000, 120000, 280000, 220000, 130000, 320000],
    "Income": [80000, 60000, 50000, 100000, 70000, 40000, 90000, 75000, 45000, 110000],
    "Family_Size": [3, 2, 4, 4, 3, 2, 5, 4, 3, 6],
    "Num_Bedrooms": [4, 3, 3, 5, 3, 2, 4, 3, 2, 5],
    "Yard_Space": ["Medium", "Small", "Large", "Medium", "Small", "Large", "Medium", "Medium", "Large", "Medium"],
    "Distance_to_Stores": [1.5, 0.5, 3.0, 2.0, 0.3, 4.0, 1.0, 0.8, 5.0, 1.2],
    "Crime_Rate": ["Low", "Medium", "Low", "Low", "High", "Medium", "Low", "Medium", "Medium", "Low"],
    "Amenities": ["Low", "Medium", "Low", "Low", "High", "Medium", "Low", "Medium", "Medium", "Low"],
    "Square_Footage": [2200, 1600, 1800, 2800, 1500, 1200, 2400, 1800, 1300, 3000],
    "Climate": ["Mild", "Temperate", "Harsh", "Mild", "Temperate", "Harsh", "Mild", "Temperate", "Harsh", "Mild"],
    "Transport_Access": ["Excellent", "Good", "Poor", "Excellent", "Good", "Poor", "Excellent", "Good", "Poor", "Excellent"]
}

df = pd.DataFrame(data)

def extract_data():
    """Task to extract data from the sample dataset."""
    return df

def clean_data(df):
    """Task to clean the dataset (e.g., remove duplicates, handle missing values)."""
    cleaned_df = df.drop_duplicates()  # Remove duplicate rows
    cleaned_df.fillna(0, inplace=True)  # Replace missing values with 0 (for illustration)
    return cleaned_df

def analyze_data(cleaned_df):
    """Task to analyze the dataset and identify key factors influencing home selection."""
    # Perform analysis to determine factors affecting home suitability and value
    important_factors = cleaned_df[["Price", "Num_Bedrooms", "Distance_to_Stores", "Amenities", "Transport_Access"]]
    correlation_matrix = important_factors.corr()  # Calculate correlation matrix
    return correlation_matrix

def model_data(cleaned_df):
    """Task to model the dataset and predict future home values."""
    # Example: Fit a regression model to predict home values
    X = cleaned_df[["Price", "Num_Bedrooms", "Distance_to_Stores", "Amenities", "Transport_Access"]]
    y = cleaned_df["Income"]
    model = YourRegressionModel()  # Instantiate your regression model
    model.fit(X, y)  # Fit the model
    return model

# Define Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': False
}

dag = DAG('home_selection_pipeline', 
          default_args=default_args, 
          description='Pipeline for selecting the best affordable home',
          schedule_interval=None
)

# Define tasks using PythonOperator
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag
)

model_task = PythonOperator(
    task_id='model_data',
    python_callable=model_data,
    dag=dag
)

# Define task dependencies
extract_task >> clean_task >> analyze_task >> model_task

# Optionally, define additional tasks and dependencies as needed

# Set up DAG structure and dependencies
dag
