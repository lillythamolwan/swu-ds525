import psycopg2

    
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
        BASKET_ID bigint,
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
        BASKET_ID bigint,
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


# Copy data from S3 to the table we created above
copy_table_query = [ """
    COPY transaction_data FROM 's3://lillythamolwan-titanic/transaction_data.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOV2L5FOJ'
    SECRET_ACCESS_KEY 'hCSfj9QA2s5iEQaWDhrN0hjxcHzqd+MaJRHhgn/z'
    SESSION_TOKEN 'FwoGZXIvYXdzEAMaDCm8t184+uAeMH6RjCLKAaMe5hgpnWQY/b/do1xFnjS13e8mR50kZGtw7qD81vEegS+9dOkOU1PF1ohPLhixqqdy+f0Gp8E56OaGEN5drYrhoPm4jLsTRnkHr3sGEbDq5cFin3P50V8nBqq8m9dkmbegAGWg9VIEqvMTBY4pLf8ZXHIcUM/fLYmtgaA6omvzF0xxNjNspW73X0ocm7mWNxNFzmL9zJp7oi7vwnYmM44NYBzOgmnO7k2zDFfY2UFIQYLMoNUkZOX//6oCxotC4QL6vyBLUlRCGdko7JT2nAYyLb1KKwQXrN6uupX/kWez1qiaazPtxBC9zLgATBPnxesmD6WalmPGTZRSIHt06A=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY product FROM 's3://lillythamolwan-titanic/product.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOV2L5FOJ'
    SECRET_ACCESS_KEY 'hCSfj9QA2s5iEQaWDhrN0hjxcHzqd+MaJRHhgn/z'
    SESSION_TOKEN 'FwoGZXIvYXdzEAMaDCm8t184+uAeMH6RjCLKAaMe5hgpnWQY/b/do1xFnjS13e8mR50kZGtw7qD81vEegS+9dOkOU1PF1ohPLhixqqdy+f0Gp8E56OaGEN5drYrhoPm4jLsTRnkHr3sGEbDq5cFin3P50V8nBqq8m9dkmbegAGWg9VIEqvMTBY4pLf8ZXHIcUM/fLYmtgaA6omvzF0xxNjNspW73X0ocm7mWNxNFzmL9zJp7oi7vwnYmM44NYBzOgmnO7k2zDFfY2UFIQYLMoNUkZOX//6oCxotC4QL6vyBLUlRCGdko7JT2nAYyLb1KKwQXrN6uupX/kWez1qiaazPtxBC9zLgATBPnxesmD6WalmPGTZRSIHt06A=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY hh_demographic FROM 's3://lillythamolwan-titanic/hh_demographic.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOV2L5FOJ'
    SECRET_ACCESS_KEY 'hCSfj9QA2s5iEQaWDhrN0hjxcHzqd+MaJRHhgn/z'
    SESSION_TOKEN 'FwoGZXIvYXdzEAMaDCm8t184+uAeMH6RjCLKAaMe5hgpnWQY/b/do1xFnjS13e8mR50kZGtw7qD81vEegS+9dOkOU1PF1ohPLhixqqdy+f0Gp8E56OaGEN5drYrhoPm4jLsTRnkHr3sGEbDq5cFin3P50V8nBqq8m9dkmbegAGWg9VIEqvMTBY4pLf8ZXHIcUM/fLYmtgaA6omvzF0xxNjNspW73X0ocm7mWNxNFzmL9zJp7oi7vwnYmM44NYBzOgmnO7k2zDFfY2UFIQYLMoNUkZOX//6oCxotC4QL6vyBLUlRCGdko7JT2nAYyLb1KKwQXrN6uupX/kWez1qiaazPtxBC9zLgATBPnxesmD6WalmPGTZRSIHt06A=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
    """
    COPY status FROM 's3://lillythamolwan-titanic/status.csv'
    ACCESS_KEY_ID 'ASIAYXMWM46LOV2L5FOJ'
    SECRET_ACCESS_KEY 'hCSfj9QA2s5iEQaWDhrN0hjxcHzqd+MaJRHhgn/z'
    SESSION_TOKEN 'FwoGZXIvYXdzEAMaDCm8t184+uAeMH6RjCLKAaMe5hgpnWQY/b/do1xFnjS13e8mR50kZGtw7qD81vEegS+9dOkOU1PF1ohPLhixqqdy+f0Gp8E56OaGEN5drYrhoPm4jLsTRnkHr3sGEbDq5cFin3P50V8nBqq8m9dkmbegAGWg9VIEqvMTBY4pLf8ZXHIcUM/fLYmtgaA6omvzF0xxNjNspW73X0ocm7mWNxNFzmL9zJp7oi7vwnYmM44NYBzOgmnO7k2zDFfY2UFIQYLMoNUkZOX//6oCxotC4QL6vyBLUlRCGdko7JT2nAYyLb1KKwQXrN6uupX/kWez1qiaazPtxBC9zLgATBPnxesmD6WalmPGTZRSIHt06A=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """,
]

insert_dwh_queries = [
    """
    INSERT INTO sales
    SELECT transaction_data.household_key,
        transaction_data.PRODUCT_ID,
        BASKET_ID,
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
    for query in create_table_query:
        cur.execute(query)
        conn.commit()

def copy_table (cur, conn):
    for query in copy_table_query :
        cur.execute(query)
        conn.commit()

def _insert_dwh_tables(cur, conn):
    for query in insert_dwh_queries:
        cur.execute(query)
        conn.commit()

def main():
    host = "redshift-cluster-1.cxmo4um5uuoa.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Lilly2022"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn) 
    copy_table(cur, conn) 
    _insert_dwh_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()