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
    "DROP TABLE IF EXISTS status",
    "DROP TABLE IF EXISTS sales",
]

# Create table
    create_table_query = [
    """
    CREATE TABLE IF NOT EXISTS transaction_data (
        household_key int,
        BASKET_ID int,
        DAY int,
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
    CREATE TABLE IF NOT EXISTS status (
        Status text,
        Count int
        )
    """,
    """
    CREATE TABLE IF NOT EXISTS sales (
        household_key int,
        PRODUCT_ID int,
        SALES_VALUE decimal,
        STORE_ID int,
        WEEK_NO int,
        DEPARTMENT text,
        AGE_DESC text,
        MARITAL_STATUS_CODE text,
        INCOME_DESC text,
        HOMEOWNER_DESC text,
        HOUSEHOLD_SIZE_DESC text,
        Status text,
        Count int
        )
    """
]

    cur.execute(create_table_query)
    conn.commit()

# Copy data from S3 to the table we created above
    copy_table_query = """
    COPY transaction_data FROM 's3://lillythamolwan-titanic/transaction_data.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOPBQBY6N'
    SECRET_ACCESS_KEY 'BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX'
    SESSION_TOKEN 'FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY product FROM 's3://lillythamolwan-titanic/product.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOPBQBY6N'
    SECRET_ACCESS_KEY 'BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX'
    SESSION_TOKEN 'FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY hh_demographic FROM 's3://lillythamolwan-titanic/hh_demographic.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOPBQBY6N'
    SECRET_ACCESS_KEY 'BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX'
    SESSION_TOKEN 'FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY status FROM 's3://lillythamolwan-titanic/status.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOPBQBY6N'
    SECRET_ACCESS_KEY 'BrRNa3NEvgQMJcLB2hkPm73CK+NpdqnLasiJuVnX'
    SESSION_TOKEN 'FwoGZXIvYXdzEPD//////////wEaDErk3focojBIrYFhtSLKAVrxF2uz8+NXjKVGDuxngA6lsfAtKqhxlCVGArook8IIK6oH1xsZxaKimppWHsZGP1sUbUSElIvEJ2T3S2+h1KNkoPDDkDdZO8tZ5qGRnvuzORhSyxN4VWBpeIC112y0gqVAH4BUT8zdAoaSCLTnZESZomsniyxuT7C3PxJeiwJAYDnmsS7U95CWBFEhO1rw2r/1W9s/wFAQZYPYunBzvvVgqnsOjkLvuPXPNdQI4WDzwEkgL5nkO3TgOE+sajFqbV3rk8rpF8LHIjYou/3xnAYyLQoJpAgdeligyBg51zI1Spcd6I6vBAfPTYx2chOhfhPnySJFwM39NjnB1LZNrA=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,

    cur.execute(copy_table_query)
    conn.commit()

    conn.close()

insert_dwh_queries = [
    """
    INSERT INTO sales
    SELECT household_key,
        PRODUCT_ID,
        SALES_VALUE,
        STORE_ID,
        WEEK_NO,
        DEPARTMENT
    FROM transaction_data
    INNER JOIN product
        ON transaction_data.PRODUCT_ID = product.PRODUCT_ID
    INNER JOIN hh_demographic
        ON hh_demographic.household_key = transaction_data.household_key
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


def _load_staging_tables():
    for query in copy_table_queries:
        cur.execute(query.format(curr_date, access_key_id, secret_access_key, session_token))
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def _insert_dwh_tables():
    for query in insert_dwh_queries:
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