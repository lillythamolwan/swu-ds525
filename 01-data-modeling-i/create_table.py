import psycopg2


table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actors = "DROP TABLE IF EXISTS actors CASCADE"
table_drop_repos = "DROP TABLE IF EXISTS repos CASCADE"

table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors (
        actor_id INT PRIMARY KEY, 
        login VARCHAR(100) NOT NULL
        
    )
"""

table_create_events = """
    CREATE TABLE IF NOT EXISTS events (
        events_id TEXT PRIMARY KEY, 
        type VARCHAR(200) NOT NULL,
        actor_id INT NOT NULL,
        FOREIGN KEY (actor_id)
        REFERENCES actors (actor_id)

    )
"""
table_create_repos = """
    CREATE TABLE IF NOT EXISTS repos (
        repo_id INT PRIMARY KEY, 
        name VARCHAR(100) NOT NULL,
        url VARCHAR(200) NOT NULL,
        actor_id INT NOT NULL,
        FOREIGN KEY (actor_id)
        REFERENCES actors (actor_id)
    )
"""

create_table_queries = [
    table_create_actors, table_create_events, table_create_repos
]
drop_table_queries = [
    table_drop_events, table_drop_actors, table_drop_repos
]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn) 
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()