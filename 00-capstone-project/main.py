import boto3

aws_access_key_id = "1"
aws_secret_access_key = "2"
aws_session_token = "3"

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

s3.meta.client.upload_file(
    "status.csv",
    "lillythamolwan-titanic",
    "status.csv",
)
