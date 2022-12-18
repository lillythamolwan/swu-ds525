import boto3

aws_access_key_id = "ASIAYXMWM46LOD7FWLHI"
aws_secret_access_key = "o4M+3LmtwyoVxUjTyTz5XBQPy/nh0emPS2j9a9Oi"
aws_session_token = "FwoGZXIvYXdzEB0aDN8bybirReyfpZXmLCLKAaim9wDJwxxibvX5fnQEelERSgU3B8OgAGcrjJ4GghLP0X/Ccp/hyZ+CGYnBgKqwz1swVWAMpqgBQDxeclzHgQHHt9hbMEjdzLbJIEHRBxJvabGzZu3jzW35tHe9KDgHidIF0DjJNgwYlJsDNZjaf/JBA3OFZuqCMXYzwmfJ9AZRLgqplnLvrM72qA1zq/vkt9t6u7wl/jxigjdM73plj5GTn12R8vpe+mFJ2F2+Kf2YDInC/pM+ciNkhzVzdwc7ErTd0emUX7gWF44o9+37nAYyLRqKLPtBOEWXcDFw/KyM7ikw6dwxL6ZAKHvqLA4zvvWIS6sO3Bq0+PU8F9cY5A=="

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
