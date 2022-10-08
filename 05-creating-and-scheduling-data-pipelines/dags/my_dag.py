from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
#ตั้งชื่อฟังก์ชันชื่อเดียวกับไฟล์
with DAG(
    "my_dag", 
    start_date=timezone.datetime(2022, 10, 8),
    schedule=None,
    tags=["workshop"],
):
    t1 = EmptyOperator(task_id="t1") #ตั้งชื่อเดียวกับinstance
    t2 = EmptyOperator(task_id="t2") #ตั้งชื่อเดียวกับinstance

    t1 >> t2