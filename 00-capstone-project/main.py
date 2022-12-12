import boto3

aws_access_key_id = "ASIAYXMWM46LPU36XUN5"
aws_secret_access_key = "JQaUhLh2Qqgiwyw1yPBo0I2UcdbDtvj1eXQUdYcP"
aws_session_token = "FwoGZXIvYXdzEJL//////////wEaDKhZFxxiZkD5Nc4bMSLKAWZ/6f1IgKPenyfWUHNNzAPPt9DDVo/uKHx+1LNTi01obT3nnqMwk6I15G96vGHxMXSpak5edS/T/OCXtyzBPVctZsQ7LtKCGeFWSfB/mN7Qhh1XyvRLzPOtJRhcH98Y5bzYUrF1snB38XyBCB5ViWXPkTvVZIKCqSW5CzgDILELGZZE6cByO1INHKe8OJl2GWA4StXmBpORQi3U8wSa221/KKKaQSMn3r6Hd/LS+6aRuSOwYZwgyT0sg4pNqVhJaUI5ET4CvWy/ltIomqfdnAYyLcv7EuN79RAlqykrhUkAnFxPTN+zxDB17jB7C5dv14ZqWZvGUK9QtbN26ELRIg=="

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
    "Count of BASKET_ID by Department.csv",
    "lillythamolwan-titanic",
    "Count of BASKET_ID by Department.csv",
)
