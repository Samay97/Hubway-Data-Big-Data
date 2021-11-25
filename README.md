# Hubway-Data-Big-Data
DHBW Projekt für die Vorlesung Big Data mit den Hubway-Daten

## Ausfürhren des DAG´s

In der Konsole ausführen:
```
docker compose build

docker compose up
```

nach dem startup
```
docker exec -it hadoop bash
sudo su hadoop
start-all.sh
hiveserver2
```
Das Terminal mit dem hiveserver2 offen lassen

Im Browoser folgende url aufrufen: http://localhost:8080/admin und den DAG 'Hubway-Data' ausführen

## Aufbau



### Airflow Tasks

|Task                               |Funktionalität|
|-----------------------------------|---|
|create_import_dir                  |   |
|clear_import_dir                   |   |
|download-task-id                   |   |
|remove-zip-file                    |   |
|get-downloaded-filenames           |   |
|mkdir-hdfs-hubway-data-dir-hiveSQL |   |
|mkdir-hdfs-hubway-data-dir-raw     |   |
|mkdir-hdfs-hubway-data-dir-final   |   |
|mkdir-hdfs-hubway-data-dir-kpis    |   |
|create_hubway_datata_table         |   |
|upload-hubway-data-to-hdfs-raw     |   |
|wait_for_upload_hdfs               |   |
|pyspark_csv_optimize               |   |
|wait_for_csv_optimize              |   |
|create_kpis_dir                    |   |
|clear_kpis_dir                     |   |
|upload_to_hive_database            |   |
|pyspark_calculate_kpis             |   |
|wait_for_calculate_kpis            |   |
|get_calculated_kpis                |   |
|create_final_excel_kpis            |   |



