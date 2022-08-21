import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import csv

from airflow.utils.timezone import datetime
from kafka import KafkaProducer,KafkaConsumer

def producer():
    my_producer = KafkaProducer(
        bootstrap_servers=['192.168.193.118 : 9092'],
        value_serializer=lambda x: bytearray(x, 'utf-8')
    )

    with open("/home/chibm/airflow/dags/lineid.txt", "r") as line_File:
#     with open("E:/FinalProject-Masterdev-Domain_DATA/CrawlData/lineid.txt", "r") as line_File:
        line_id = int(line_File.read())

# with open("/home/lamnv155/airflow/dags/huyvv20/data_tiki_final.csv", "r", encoding="utf-8") as csv_File:
    i = 0
    # with open("E:/FinalProject-Masterdev-Domain_DATA/CrawlData/data_tiki_final.csv", "r", encoding="utf-8") as csv_File:
    with open("/home/chibm/airflow/dags/final_tiki_data.csv", "r", encoding="utf-8") as csv_File:
        while(i<line_id):
            line = csv_File.readline()
            i += 1
            # my_producer.send(topic="huyvv20_tiki", value=line[0:])
        for j in range(50):
            line = csv_File.readline()
            my_producer.send(topic="huyvv20", value=line[0:])
            i += 1
                # for j in range(50):
                #     # my_producer.send(topic="huyvv20_tiki", value=line[1:])
                #     print(line[0:])
                #     line = csv_File.readline()
    csv_File.close()
    # with open("/home/lamnv155/airflow/dags/huyvv20/lineid.txt", "w") as line_File:
    # with open("E:/FinalProject-Masterdev-Domain_DATA/CrawlData/lineid.txt", "w") as line_File:
    with open("/home/chibm/airflow/dags/lineid.txt", "w") as line_File:
        line_File.write(str(i))

with DAG(
        default_args={
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        dag_id='producer_tiki_data',
        start_date=datetime(2022, 8, 20),
        schedule_interval=timedelta(seconds=5),
        catchup=False,
        ) as dag:
            t1 = PythonOperator(
                task_id='kafka_producer',
                python_callable=producer
            )




