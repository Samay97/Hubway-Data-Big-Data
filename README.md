# Hubway-Data-Big-Data
DHBW Project for Big Data with the Hubway dataset

## How to start

Just run:
```
docker compose build

docker compose up
```

after all contaier start up is done

```
docker exec -it hadoop bash
sudo su hadoop
start-all.sh
hiveserver2
```
and let hiveserver2 terminal open

navigate to http://localhost:8080/admin and see Hubway-Data DAG