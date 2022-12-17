import boto3

aws_access_key_id = "ASIAYXMWM46LDYEIP64Q"
aws_secret_access_key = "4fzb9wMYQR1Pi8Sj2ngxsiX7GnZP4TIf0hcMGUpq"
aws_session_token = "FwoGZXIvYXdzEAIaDEZeK+Fx++4NPtNDQCLKAbfbkmB4oTIVyx6w6Crd+hi5SzhODH1EGnajzT/PC2AUznEKHzRhhhZPElTxtbdRLOaX80FZoel6ldMM8T0La8OLpsHzj/7lJPT+1ymRR6cyPy1zMfj8aprtnhxCRARWNScuoKpRNY1g1coJnGElUrfV0Po7l4P+NwlIDfLWzh/bh075ZdrwKJH3xPIear7wBqGI5nOzuFvNztrfOFLNB5DkgvpeZ4DfJbV3w9+WSr8Z6cpmUZ6j+kk8Xn7nTWiLO3eyv+nNPmUAUrkolIv2nAYyLRDWymbePpGqwDeYuzRMus0D2Xn+iTv/9kdhZUYGV4SbGzvDSjVuA/pKaEsGEA=="

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
