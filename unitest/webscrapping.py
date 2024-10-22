from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mmobomb_game_data',
    default_args=default_args,
    description='Fetch game data from MMOBomb API and save to PostgreSQL',
    schedule_interval='0 10 * * *',
    catchup=False,
)

def fetch_game_data():
    api_url = 'https://www.mmobomb.com/api1/games' # Replace with actual API URL
    
    response = requests.get(api_url)
    game_data = response.json()
    
    return game_data

def save_to_postgres(**kwargs):
    game_data = kwargs['ti'].xcom_pull(task_ids='fetch_game_data_task')
    
    conn = psycopg2.connect(
        dbname='airflow',
        user='postgres',
        password='your_db_password',
        host='localhost',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    for game in game_data:
        query = """
        INSERT INTO games (game_name, release_date, genre)
        VALUES (%s, %s, %s)
        """
        cursor.execute(query, (game['name'], game['release_date'], game['genre']))
    
    conn.commit()
    conn.close()

fetch_game_data_task = PythonOperator(
    task_id='fetch_game_data_task',
    python_callable=fetch_game_data,
    dag=dag,
)



fetch_game_data_task 
