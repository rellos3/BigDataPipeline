import json
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG


def get_data():
    url = "https://api.census.gov/data/2022/acs/acs5/pums?get=RMSP,HINCP,NPF,JWMNP,AGEP,DIS,SEX,MAR,COW,JWTRNS&ucgid=0400000US06"
    # rooms, income, people in family, commute time, age, disability, sex, marital status, worker class, transportation
    response = requests.get(url)
    print(response.status_code)

    census = response.text
    census = json.loads(census)
    df = pd.DataFrame.from_dict(census)

    conn = sqlite3.connect("/opt/airflow/dags/census_data.db")
    df.to_sql("data", conn, if_exists="replace")
    print("census database saved")
    cursor = conn.cursor()
    print(cursor.execute("SELECT FIRST RMSP FROM data"))


def format_df():
    conn = sqlite3.connect("census_data.db")
    df = pd.read_sql("SELECT * FROM data", conn)
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    # rooms, income, people in family, commute time, age, disability, sex, marital status, worker class, transportation
    df.columns = [
        "ROOMS",
        "INCOME",
        "FAMILY_COUNT",
        "COMMUTE",
        "AGE",
        "DISABILITY",
        "SEX",
        "MARITAL-STATUS",
        "WORK_CLASS",
        "TRANSPORTATION",
        "ST",
    ]

    print(df.head())

    int_list = [
        "ROOMS",
        "INCOME",
        "FAMILY_COUNT",
        "COMMUTE",
        "AGE",
        "DISABILITY",
        "SEX",
        "MARITAL-STATUS",
        "WORK_CLASS",
        "TRANSPORTATION",
    ]

    for x in int_list:
        df[x] = df[x].astype(int)

    # Check new datatypes:
    print(df.info())
    df.to_csv("census.csv", index=False)


dag = DAG(
    "analyze_cenus_housing",
    default_args={
        "start_date": days_ago(1),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 22 * * *",
    catchup=False,
)


get_data_task = PythonOperator(task_id="get_data", python_callable=get_data, dag=dag)

#format_df_task = PythonOperator(
#   task_id="format_dataframe", python_callable=format_df, dag=dag
#)

get_data_task
