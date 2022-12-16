import boto3

aws_access_key_id = ""
aws_secret_access_key = ""
aws_session_token = ""

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
    "superstore.csv",
    "lillythamolwan-titanic",
    "superstore.csv",
)

import psycopg2


def main():
    host = ""
    dbname = "dev"
    user = "awsuser"
    password = ""
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS employees"
    cur.execute(drop_table_query)
    conn.commit()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS employees (
        name text,
        salary int
    )
    """
    cur.execute(create_table_query)
    conn.commit()

    # Copy data from S3 to the table we created above
    copy_table_query = """
    COPY employees FROM 's3://zkan-swu-labs/employees.csv'
    ACCESS_KEY_ID ''
    SECRET_ACCESS_KEY ''
    SESSION_TOKEN ''
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_query)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()

