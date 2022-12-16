import boto3

aws_access_key_id = "ASIAYXMWM46LOPBQBY6N"
aws_secret_access_key = "BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX"
aws_session_token = "FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=="

#client = boto3.client(
#    "s3",
#    aws_access_key_id=aws_access_key_id,
#    aws_secret_access_key=aws_secret_access_key,
#    aws_session_token=aws_session_token
#)
#print(client)

s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,    
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
s3.meta.client.upload_file(
    "transaction_data.csv",
    "lillythamolwan-titanic",
    "transaction_data.csv",
)

s3.meta.client.upload_file(
    "product.csv",
    "lillythamolwan-titanic",
    "product.csv",
)

s3.meta.client.upload_file(
    "hh_demographic.csv",
    "lillythamolwan-titanic",
    "hh_demographic.csv",
)

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
    
    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS sales"
    cur.execute(drop_table_query)
    conn.commit()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        STORE_ID INT PRIMARY KEY, 
        SALES_VALUE FLOAT 
    )
    """
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