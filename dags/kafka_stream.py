import json
import logging
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    "owner": "arthemyst",
    "start_date": datetime(2024, 6, 2, 20, 00)
}


def get_data(api_url: str = "https://randomuser.me/api/") -> dict:
    api_response = requests.get(api_url)
    api_response = api_response.json()
    api_response = api_response['results'][0]
    return api_response


def format_data(response: dict) -> dict[str, str]:
    location = response["location"]
    data = {
        "first_name": response["name"]["first"],
        "last_name": response["name"]["last"],
        "gender": response["gender"],
        "address": f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        "email": response["email"],
        "username": response["login"]["username"],
        "dob": response["dob"]["date"],
        "registered_date": response["registered"]["date"],
        "phone": response["phone"],
    }

    if "postcode" in response:
        data["postcode"] = response["postcode"]

    if "picture" in response:
        data["picture"] = response["picture"]["medium"]

    return data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            response = get_data()
            response = format_data(response)
            producer.send('user_created', json.dumps(response).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG("user_automation",
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as directed_acyclic_graph:
    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data
    )
