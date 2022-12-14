import psycopg2

#Drop table ใช้สำหรับล้าง Table เดิมเพื่อรันในครั้งต่อไป
drop_table_queries = [
    "DROP TABLE IF EXISTS staging_events",
    "DROP TABLE IF EXISTS Repo",
    "DROP TABLE IF EXISTS Actor",
    "DROP TABLE IF EXISTS Event",
]



#สร้างตัวแปรเก็บ sql สร้างตาราง

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        actor_id bigint,
        actor_login text,
        actor_url text,
        repo_id bigint,
        repo_name text,
        repo_url text,
        public boolean,
        created_at text,
        actor_display_login text,
        actor_gravatar_id text,
        push_id text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Event (
        id text,
        type text ,
        public boolean,
        create_at text,
        repo_id bigint,
        repo_name text,
        actor_id bigint,
        actor_login text,
        push_id text

        )
    """,
    """
    CREATE TABLE IF NOT EXISTS Actor (
        id bigint,
        login text,
        display_login text,
        gravatar_id text,
        url text
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


copy_table_queries = [
    """
    COPY staging_events FROM 's3://mylilly/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::600000227222:role/LabRole'
    JSON 's3://mylilly/events_json_path.json'
    REGION 'us-east-1'
    """,
]

insert_table_queries = [
"""
    INSERT INTO Repo (id,name,url) 
    SELECT DISTINCT repo_id, repo_name, repo_url
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM Repo)
    """,
    """
    INSERT INTO Actor (id,login,display_login,gravatar_id,url)
    SELECT DISTINCT actor_id,actor_login, actor_display_login,actor_gravatar_id, actor_url
    FROM staging_events
    WHERE actor_id NOT IN (SELECT DISTINCT id FROM Actor)
    """,
    """
    INSERT INTO Event (id,type,public,create_at,repo_id,repo_name,actor_id,actor_login,push_id)
    SELECT DISTINCT id, type, public,created_at,repo_id,repo_name,actor_id,actor_login,push_id
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM Event)
    """,
]

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.cxmo4um5uuoa.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Lilly230922"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    #drop_tables(cur, conn)
    #create_tables(cur, conn)
    #load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    # query data
    query = "select * from staging_events"
    cur.execute(query)
    # print data
    records = cur.fetchall()
    for row in records:
        print(row)
    conn.close()


if __name__ == "__main__":
    main()