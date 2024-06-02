import json
from datetime import datetime

import requests
from requests import Response
# from airflow import DAG
# from airflow.operators.python import PythonOperator

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
    response = get_data()
    response = format_data(response)
    print(json.dumps(response, indent=3))


# with DAG("user_automation",
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as directed_acyclic_graph:
#     streaming_task = PythonOperator(
#         task_id="stream_data_from_api",
#         python_callable=stream_data
#     )

stream_data()
