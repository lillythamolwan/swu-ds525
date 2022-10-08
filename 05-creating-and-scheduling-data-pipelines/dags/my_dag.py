from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
#ตั้งชื่อฟังก์ชันชื่อเดียวกับไฟล์
with DAG(
    "my_dag", 
    start_date=timezone.datetime(2022, 10, 8),
    schedule="0 0 * * *",
    tags=["workshop"],
):
    t1 = EmptyOperator(task_id="t1") #ตั้งชื่อเดียวกับinstance

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )

    def _print_hey():
        print("Hey!")

    print_hey = PythonOperator(
    task_id="print_hey", #ตั้งชื่อฟังก์ชัน
    python_callable=_print_hey,
)
    t2 = EmptyOperator(task_id="t2") #ตั้งชื่อเดียวกับinstance

    t1 >> echo_hello >> print_hey >> t2

    # t1 >> echo_hello 
    # t1 >> print_hey
    # echo_hello >> t2
    # print_hey  >> t2

    # t1 >> [echo_hello, print_hey] >> t2

    