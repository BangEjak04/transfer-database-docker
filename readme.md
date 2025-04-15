docker run --name mysql-db -e MYSQL_ROOT_PASSWORD=root -d -p 3306:3306 mysql:latest
docker run --name mongo-db -d -p 27017:27017 mongo:latest
docker run --name postgres-db -e POSTGRES_PASSWORD=root -d -p 5433:5432 postgres:latest
docker run --name cassandra-db -d -p 9042:9042 cassandra:latest


biarkan booting sekitar 2mnt