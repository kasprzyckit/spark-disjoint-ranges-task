# Spark - Disjoint ranges

## Running the application
To set up a local Spark cluster and submit the Scala application to it run:
```
docker-compose up
```
The input data is expected to be stored in `IP_RANGES` table existing in `spark-disjoint-ranges-task_spark-postgres-data` Docker volume.
If the volume with the data is not initialized, example input data is used instead.

## Generating input data
To generate data, **before** you run the application build the generator image with:
```
docker-compose build
docker build -t spark-disjoint-generator -f generator/Dockerfile .
```

Then run a standalone Postgres server and the generator app:
```
docker run --name generator-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=1234 -v  spark-disjoint-ranges-task_spark-postgres-data:/var/lib/postgresql/data --rm -d postgres:10
docker run -e IP_RANGES_COUNT=10 --link generator-postgres:postgres --rm spark-disjoint-generator
```
This will insert `IP_RANGES_COUNT` rows of data, containing randomly generated ranges of IPs, each of them covering up to 100000 IP addresses.

Afterwards, stop the Postgres server:
```
docker container stop generator-postgres
```
Now, run the cluster.

## Results
Query the elasticsearch server for the results:
```
curl http://localhost:9200/disjoint/_search?size=100
```