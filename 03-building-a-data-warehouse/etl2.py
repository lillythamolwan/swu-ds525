CREATE TABLE IF NOT EXISTS Actor (
        id bigint,
        login text,
        display_login text,
        gravatar_id text,
        url text
    )


copy_table_queries = [
    """
    COPY Actor FROM 's3://lilly-my-json/events_json_path.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::600000227222:role/LabRole'
	JSON 's3://lilly-my-json/events_json_path.json'
    REGION 'us-east-1' 
    """,
]
