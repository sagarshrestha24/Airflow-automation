import requests
import json
from datetime import datetime, timedelta
from base64 import b64encode
import os


def send_slack_alert(error):
    slack_hook = 'enter your slack webhook url'    
    if len(error) == 0:
      slack_data={"Airflow Errored Dags are": error}
      post = {	"text":"{0}".format(slack_data)	}
      
      response = requests.post(slack_hook, data=json.dumps(post),headers={'Content-Type': 'application/json'})
      if response.status_code!=200:
        raise ValueError('Request to slack returned an error %s, the response is:\n%s'
        % (response.status_code, response.text))
      print("No Errored dags found")
    else:
      print("sending slack alert")
      slack_data={"Airflow Errored Dags are": error}
      post = {	"text":"{0}".format(slack_data)	}
      response = requests.post(slack_hook, data=json.dumps(post),headers={'Content-Type': 'application/json'})
      if response.status_code!=200:
        raise ValueError('Request to slack returned an error %s, the response is:\n%s'
        % (response.status_code, response.text))
    



#This is to get the list of all the DAGs from airflow..
def list_dags():
  url = "https://airflow.com/api/v1/dags"                ## Enter Your Airflow URL
  payload = json.dumps({
    'only_active':'True',
  })
  airflow_username= "Airflow Username"
  airflow_password= "Airflow Password"
  base64_bytes = b64encode(
                        (f"{airflow_username}:{airflow_password}").encode("ascii")
                        ).decode("ascii")
  headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Basic {base64_bytes}'
            }
  DAG_response = requests.request("GET", url, headers=headers, data=payload)
  dag_id=[x['dag_id'] for x in DAG_response.json()['dags']]
  print(dag_id)

  todays_time= todays_time = datetime.today().strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
  yesterday=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')

  url = "https://airflow.com/api/v1/dags/~/dagRuns/list"   ## Enter Your Airflow URL
  payload = json.dumps({
    'dag_ids': dag_id,
    'states':['failed'],
    'start_date_gte': yesterday,
    'end_date_lte': todays_time,
  })
  headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Basic {base64_bytes}'
            }

  DAG_response = requests.request("POST", url, headers=headers, data=payload)
  dag_ids=[x['dag_id'] for x in DAG_response.json()['dag_runs']]
  #print(type(dag_ids))
  send_slack_alert(dag_ids)

list_dags()
                 
            
#send_slack_alert(DAG_response.json())


#print(DAG_response.json())
