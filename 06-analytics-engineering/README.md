# Analytics Engineering

เข้ามาที่โฟลเดอร์
cd 06-analytics-engineering

```sh
docker-compose up
```
Open port 3000 -> SQLPad
User : admin@swu.ac.th
Pass : admin

Test Query on SQLPad
![sqlpad_query_graph](sqlpad_query_graph.jpg)

Open new terminal
cd 06-analytics-engineering
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
mkdir -p ~/.dbt

Create a dbt project

```sh
dbt init
```

Edit the dbt profiles

```sh
code ~/.dbt/profiles.yml
```

```yml
jaffle:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: public

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: prod

  target: dev
```

Test dbt connection # ทดสอบ connection กับ profiles

```sh
cd jaffle
dbt debug
```

You should see "All checks passed!".

ทดลองสั่ง
```sh
dbt run
```
Create folder staging/jaffle and marts

Set up Source
- Folder staging สรา้งไฟล์ stg_jaffle__customers.sql 

-- เขียนคำสั่ง CTE => Common Table Expression เพื่อประกาศตัวแปร 
with

a as (
    select * from abc
)

, b as (
    select * from a
)

select
    *
from b

To create models

```sh
dbt run
```

To test models

```sh
dbt test
```

To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve
```

