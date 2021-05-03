from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import requests
import json
import os
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),

}


dag = DAG(
    dag_id='NYT_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2021,5,1),
    tags=['email popular books'],
)
API_KEY = 'Jy6m3w78sMXkQTkIk74m2R0TpbQU9i97'


def get_data():
    recipients = ['bestsellerbooks65@gmail.com', 'bestsellerbooks65@outlook.com']
    current = date.today() + timedelta(days=6)

    try:
        data = context["dag_run"].conf["key"]
        recipients = data
        fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/current/Combined%20Print%20and%20E-Book%20Fiction.json?api-key={API_KEY}"
        Non_fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/current/Combined%20Print%20and%20E-Book%20Nonfiction.json?api-key={API_KEY}"


    except:
        fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/{current}/Combined%20Print%20and%20E-Book%20Fiction.json?api-key={API_KEY}"
        Non_fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/{current}/Combined%20Print%20and%20E-Book%20Nonfiction.json?api-key={API_KEY}"

    requestHeaders = {
    "Accept": "application/json"
    }

    r = requests.get(fiction_url, headers=requestHeaders)
    fiction = r.json()
    r = requests.get(Non_fiction_url, headers=requestHeaders)
    Non_fiction = r.json()

    if fiction['status'] == 'OK':
        fiction_dict = {}
        Non_fiction_dict = {}

        def create_dataframe(books):
            rank = []
            book_title = []
            book_author = []
            publisher = []
            weeks_on_list = []

            for record in books["results"]["books"]:
                rank.append(record['rank'])
                book_title.append(record['title'])
                book_author.append(record['author'])
                publisher.append(record['publisher'])
                weeks_on_list.append(record['weeks_on_list'])

            book_dict = {
                'rank': rank,
                'book_title': book_title,
                'book_author':book_author,
                'publisher':publisher,
                'weeks_on_list':weeks_on_list

            }
            df = pd.DataFrame(data=book_dict,columns=['rank','book_title','book_author','publisher','weeks_on_list'])
            return df

        fiction_df = create_dataframe(fiction)
        Nonfiction_df = create_dataframe(Non_fiction)

        with open('fiction.txt', 'w') as f:
            f.write(
                fiction_df.to_string(header = False, index = False)
            )

        with open('Nonfiction.txt', 'w') as f:
            f.write(
                Nonfiction_df.to_string(header = False, index = False)
            )
        send_email(recipients,'books',"""Bestseller fictions and Nonfictions""",files=[os.getcwd() + "/fiction.txt",os.getcwd() + "/Nonfiction.txt"])


send_books = PythonOperator(
    task_id='Books',
    python_callable=get_data,
    dag=dag,
    
)

send_books