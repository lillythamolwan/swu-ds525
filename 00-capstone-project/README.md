Capstone Project DS525 Data Engineering
1. Upload data source to datalake (S3)
- ไปที่ AWS Learner Lab และที่ Terminal ให้เราพิมพ์คำสั่ง cat ~/.aws/credentials ลงไป จะได้ค่าทั้ง 3 ค่า ที่ต้องการมา ตามรูปด้านล่างนี้
![credential_aws](credential_aws.jpg)

กลับมาที่ gitpod สั่ง
cd 00-capstone-project
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
python main.py
Upload data source to datalake (S3) เข้าไปเก็บที่ Bucket เรียบร้อยแล้ว ทำการ สร้างตาราง copy insert ข้อมูลไปที่ redshift > ไฟล์ etl.py

python etl.py
cd try_redshift
code ~/.dbt/profiles.yml
ทำการตั้งค่า profile redshift

try_redshift:
  outputs:

    dev:
      type: redshift
      threads: 1
      host: Endpoint
      port: 5439
      user: awsuser
      pass: xxxxxxxx
      dbname: dev
      schema: public

  target: dev
ทดสอบ connection คำสั่ง
  dbt debug