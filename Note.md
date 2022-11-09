To install Postgres:
sudo apt install -y postgresql

To access Postgres database:
psql -h localhost -d postgres -U postgres

Commands:
\dt -> To see relations
select * from <table>;