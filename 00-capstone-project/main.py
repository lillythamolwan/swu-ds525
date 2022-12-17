import boto3

aws_access_key_id = "ASIAYXMWM46LOV2L5FOJ"
aws_secret_access_key = "hCSfj9QA2s5iEQaWDhrN0hjxcHzqd+MaJRHhgn/z"
aws_session_token = "FwoGZXIvYXdzEAMaDCm8t184+uAeMH6RjCLKAaMe5hgpnWQY/b/do1xFnjS13e8mR50kZGtw7qD81vEegS+9dOkOU1PF1ohPLhixqqdy+f0Gp8E56OaGEN5drYrhoPm4jLsTRnkHr3sGEbDq5cFin3P50V8nBqq8m9dkmbegAGWg9VIEqvMTBY4pLf8ZXHIcUM/fLYmtgaA6omvzF0xxNjNspW73X0ocm7mWNxNFzmL9zJp7oi7vwnYmM44NYBzOgmnO7k2zDFfY2UFIQYLMoNUkZOX//6oCxotC4QL6vyBLUlRCGdko7JT2nAYyLb1KKwQXrN6uupX/kWez1qiaazPtxBC9zLgATBPnxesmD6WalmPGTZRSIHt06A=="

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
