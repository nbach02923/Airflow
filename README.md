# Personal data pipeline project 
### This project docker container recommend have at least 8GB FREE of RAM not 8GB of RAM
This project container contain:
- Kafka for streaming message
- MinIO for unstructured data
- PostgreSQL for structured data
- Deltalake for processing data from MinIO
- Spark for any preprocess activities
- Airflow for orchestrate the pipeline
- Redis is not used by any other task except for message queueing for Airflow
### To run the container:
```bash
docker compose up --build -d
```
### To restart the container:
```bash
docker restart
```
### To check the container health:
```bash
docker ps
```
### To shutdown the container:
To only shut down container without remove the image and build cache
```bash
docker compose down 
```
