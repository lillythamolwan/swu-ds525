import psycopg2

def main():
    host = "redshift-cluster-1.cxmo4um5uuoa.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Lilly2022"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
#Drop table ใช้สำหรับล้าง Table เดิมเพื่อรันในครั้งต่อไป
    drop_table_queries = [
    "DROP TABLE IF EXISTS transaction_data",
    "DROP TABLE IF EXISTS product",
    "DROP TABLE IF EXISTS hh_demographic",
    "DROP TABLE IF EXISTS sales",
]

# Create table
    create_table_query = [
    """
    CREATE TABLE IF NOT EXISTS transaction_data (
        PRODUCT_ID int, 
        QUANTITY int,
        SALES_VALUE decimal,
        STORE_ID int,
        RETAIL_DISC decimal,
        TRANS_TIME int,
        WEEK_NO int,
        COUPON_DISC decimal,
        COUPON_MATCH_DISC decimal
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS product (
        PRODUCT_ID int,
        MANUFACTURER int,
        DEPARTMENT text,
        BRAND text,
        COMMODITY_DESC text,
        SUB_COMMODITY_DESC text,
        CURR_SIZE_OF_PRODUCT text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hh_demographic (
        AGE_DESC text,
        MARITAL_STATUS_CODE text,
        INCOME_DESC text,
        HOMEOWNER_DESC text,
        HH_COMP_DESC text,
        HOUSEHOLD_SIZE_DESC text,
        KID_CATEGORY_DESC text,
        household_key int
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Repo (
        id  bigint,
        name varchar,
        url varchar
        )
    """,
]

    cur.execute(create_table_query)
    conn.commit()

    # Copy data from S3 to the table we created above
    copy_table_query = """
    COPY sales FROM 's3://lillythamolwan-titanic/transaction_data.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOPBQBY6N'
    SECRET_ACCESS_KEY 'BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX'
    SESSION_TOKEN 'FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_query)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()

    import os
import glob
from sqlite3 import Timestamp
from typing import List
import json
from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook

curr_date = datetime.today().strftime('%Y-%m-%d')


create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id bigint,
        league_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS clubs (
        club_id bigint,
        club_name text,
        league_id bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS nationalities (
        nationality_id bigint,
        nationality_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id bigint,
        position_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS players (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value decimal,
        player_wage decimal,
        position_id bigint,
        nationality_id bigint,
        club_id bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_value_wage (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value decimal,
        player_wage decimal,
        position_name text,
        club_name text,
        nationality_name text,
        league_name text,
        date_oprt date
    )
    """,
]

truncate_table_queries = [
    """
    TRUNCATE TABLE leagues
    """,
    """
    TRUNCATE TABLE clubs
    """,
    """
    TRUNCATE TABLE nationalities
    """,
    """
    TRUNCATE TABLE positions
    """,
    """
    TRUNCATE TABLE players
    """,
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift
access_key_id = 'ASIAXZM22O2VTZXH3EUP'
secret_access_key = '9NwRlaistTkv5qfKIdOLVN7HN6gaKB7RLux2PLnR'
session_token = 'FwoGZXIvYXdzEO3//////////wEaDNG/m1Hu7o2WAQKniiLMAf0cWfA7AvIaERqfk3MavZ8nPJAsglvjaddRra65NP/ZpVp0225ElmUQ6MgOXxIE/wW1vH18Rf+0Wa1cp2MKAnL8GVkEH/5/t0UC8pj0GX0lRXh00WbE7mB6PbwUWhEtNbRrDtlvqys5SJfEO0T0yLoJJug3/aEMJWT462P7dB0VlYczCkFyr2xrbQStfQGLVWypcKDrOX0Q65N7S86/+DMZyM3emhUGPP9sXLEkl8eUE4IYc+3d+6KOdk8J5l3w281er4JXtpetm+6t4ijmrPGcBjItdmeSaVJ0x0fasqLnmRzMrtCNBqzQMJJ6e21kq74/cUvNQC+ejl0Mwaq/igEP'

copy_table_queries = [
    """
    COPY leagues 
    FROM 's3://jaochin-dataset-fifa/cleaned/leagues/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY clubs 
    FROM 's3://jaochin-dataset-fifa/cleaned/clubs/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY nationalities 
    FROM 's3://jaochin-dataset-fifa/cleaned/nationalities/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY positions 
    FROM 's3://jaochin-dataset-fifa/cleaned/positions/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY players 
    FROM 's3://jaochin-dataset-fifa/cleaned/players/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
]

clear_dwh_queries = [
    """
    DELETE FROM player_value_wage WHERE date_oprt = current_date
    """,
]

insert_dwh_queries = [
    """
    INSERT INTO player_value_wage 
    SELECT p.player_id
        , p.player_name
        , p.player_age 
        , p.player_overall 
        , p.player_value 
        , p.player_wage 
        , pos.position_name 
        , c.club_name 
        , n.nationality_name 
        , l.league_name 
        , current_date
    FROM players p
    INNER JOIN positions pos
        ON pos.position_id = p.position_id
    INNER JOIN nationalities n
        ON n.nationality_id = p.nationality_id
    INNER JOIN clubs c
        ON c.club_id = p.club_id
    INNER JOIN leagues l
        ON l.league_id = c.league_id
    """,
]

host = "redshift-cluster-1.c7om6vv9mbp9.us-east-1.redshift.amazonaws.com"
port = "5439"
dbname = "dev"
user = "awsuser"
password = "awsPassword1"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def _truncate_datalake_tables():
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()

def _load_staging_tables():
    for query in copy_table_queries:
        cur.execute(query.format(curr_date, access_key_id, secret_access_key, session_token))
        conn.commit()

def _clear_dwh_tables():
    for query in clear_dwh_queries:
        cur.execute(query)
        conn.commit()

def _insert_dwh_tables():
    for query in insert_dwh_queries:
        cur.execute(query)
        conn.commit()


with DAG(
    'Capstone',
    start_date = timezone.datetime(2022, 12, 1), # Start of the flow
    schedule = '@monthly', # Run once a month at midnight of the first day of the month
    tags = ['capstone'],
    catchup = False, # No need to catchup the missing run since start_date
) as dag:


    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = _create_tables,
    )

    truncate_datalake_tables = PythonOperator(
        task_id = 'truncate_datalake_tables',
        python_callable = _truncate_datalake_tables,
    )

    load_staging_tables = PythonOperator(
        task_id = 'load_staging_tables',
        python_callable = _load_staging_tables,
    )

    clear_dwh_tables = PythonOperator(
        task_id = 'clear_dwh_tables',
        python_callable = _clear_dwh_tables,
    )

    insert_dwh_tables = PythonOperator(
        task_id = 'insert_dwh_tables',
        python_callable = _insert_dwh_tables,
    )

    create_tables >> truncate_datalake_tables >> load_staging_tables >> clear_dwh_tables >> insert_dwh_tables