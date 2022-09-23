Building a Data Warehouse
1. Create cluster AWS Redshift
- go to AWS console open Redshift
- Node type = ra3.xplus 
- Number of nodes = 1
- awsuser
- set password
- click Associate IAM roles select -> LabRole and click Associate IAM roles
![image](https://user-images.githubusercontent.com/111696729/191307505-cd1eddb9-5b28-4b2d-97b1-bcb12541d152.png)
- others use defaults and click Create cluster

**go to Tab Properties -> Network and security settings
- select VPC security group
- Edit Inbound rules -> Add rule -> Type redshift -> Source IPV4address -> Save rules
- Modify publicly accessible setting -> Enable -> Save changes

![image](https://user-images.githubusercontent.com/111696729/191310902-c400f54f-7b89-436c-aeda-e02f4c7ac12d.png)
![image](https://user-images.githubusercontent.com/111696729/191310934-bc5dfc66-a7eb-4b42-9513-ae17136028ea.png)
![image](https://user-images.githubusercontent.com/111696729/191969919-a516d324-33ef-4956-a5d0-2e76773edb1d.png)
![image](https://user-images.githubusercontent.com/111696729/191970273-4ea71592-a5af-47b3-b192-3d9d83b31d68.png)


2. Create bucket S3
- go to aws console open S3 in new tab
- click create bucket
- upload json file to bucket -> Json event file, Json path file

![image](https://user-images.githubusercontent.com/111696729/191316702-96fb7663-4587-4451-a79f-eb34e377efbc.png)

- go to check IAM 
- choose Role -> LabRole
- copy ARN
![image](https://user-images.githubusercontent.com/111696729/191314225-42d53cea-2a09-46dc-b83c-befa40d73e02.png)

3. Connect database

![image](https://user-images.githubusercontent.com/111696729/191316352-caf41f65-3af8-4ee5-b587-2e324395948d.png)

4. Create table in Redshift
- go to Query editor

![image](https://user-images.githubusercontent.com/111696729/191968312-2e443d67-b540-421f-a906-30ba3106d54c.png)

5. Insert data from copying json file with json_path

![image](https://user-images.githubusercontent.com/111696729/191318324-4999c1d0-8bdc-4502-be88-cea393d68ee1.png)

6. See the result from query data -> select * from staging_events

![image](https://user-images.githubusercontent.com/111696729/191967015-c06747ea-443c-4305-84dc-b6dfa3fab8c6.png)
![image](https://user-images.githubusercontent.com/111696729/191965306-cbc7d6e4-1f40-41f6-9958-900c7e5c0c98.png)



