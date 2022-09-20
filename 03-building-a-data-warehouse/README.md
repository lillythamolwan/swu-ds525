Building a Data Warehouse
1. Go to AWS console open Redshift
Create cluster AWS Redshift

Provision (create) cluster
AQUA (Advanced Query Accelerator) turn off
Node type = ra3.xplus - the smallest
Number of node = 1
awsuser
set password
click Associate IAM roles select
 LabRole and click Associate IAM roles
Additional configurations deselect
 Use defaults
Network and security
Default VPC
VPC security groups use default
Cluster subnet group use Cluster subnet group-1 (it don't have create one in Configuration > subnet group , with all available zone)
leave the remaining default
create cluster