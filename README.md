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

Nach dem Druchlauf des Dag´s liegen die berechneten KPIs in einer Excel Datei in dem `KeyPerformanceIndicators` Ordner.


## Aufbau

Die Hubway-Daten, werden automatisiert von Kaggle herunterladen. Danach befinden diese Sich in dem Airflow-Container und werden auf HDFS hochgeladen, damit sie später mit PySpark optimiert werden können. In der Optimierung werden nicht wichtige Daten gelöscht und Daten, die für die Berechnung der KPIs benötigt werden, werden optimiert.
Danach speichert PySpark diese optimierten Daten als CSV Dateien wieder im HDFS.
Darauf werden diese in die HIVE-SQL Tabelle geladen und mit Spark werden die KPIs berechnet.
Der Airflow-Container lädt daraufhin die KPIs im CSV Format herunter und erstellt eine Excel, die so abgelegt wird, dass das Volume, das in dem Docker-Container Konfiguriert ist, die Excel Datei im Host System des Benutzers ablegt.


### Airflow Tasks

|Task                               |Funktionalität|
|-----------------------------------|---|
|create_import_dir                  |Erstellt einen Ordner zum Herunterladen in dem Airflow Container|
|clear_import_dir                   |Löscht den Inhalt des Download Ordners im Airflow Container|
|download-task-id                   |Lädt die Hubway-Daten von Kaggle herunter und entpackt diese|
|remove-zip-file                    |Entfernt die nicht mehr benötigte ZIP-Datei von dem Download|
|get-downloaded-filenames           |Listet alle CSV Dateien auf, die heruntergeladen wurden, damit nachfolgende Operatoren die Dateien finden und durch diese iterieren können|
|mkdir-hdfs-hubway-data-dir-hiveSQL |Erstellt einen Ordner im HDFS für sie HIVE SQL Tabelle|
|mkdir-hdfs-hubway-data-dir-raw     |Erstellt einen Ordner im HDFS für die CSV-Dateien|
|mkdir-hdfs-hubway-data-dir-final   |Erstellt einen Ordner im HDFS für die optimierten CSV-Dateien|
|mkdir-hdfs-hubway-data-dir-kpis    |Erstellt einen Ordner im HDFS für die berechneten KPIs|
|create_hubway_datata_table         |Erstellt einen neue HIVE Tabelle|
|upload-hubway-data-to-hdfs-raw     |Lädt alle CSV Dateien in das HDFS, für die spätere Bearbeitung|
|wait_for_upload_hdfs               |Ein Dummy OPerator der wartet bis der Upload fertig ist|
|pyspark_csv_optimize               |Optimiert alle CSV Dateien und speichert diese wieder im HDFS ab|
|wait_for_csv_optimize              |Ein Dummy OPerator der Wartet bis die Optimierung fertig ist|
|create_kpis_dir                    |Erstellt einen Ordner zum Abspeiern der KPIs in dem Airflow Container|
|clear_kpis_dir                     |Löscht den Inhalt des KPIs Ordners im Airflow Container|
|upload_to_hive_database            |Lädt alle optimeirten CSV Dateien in die HIVE SQL Tabelle hoch|
|pyspark_calculate_kpis             |Berechnet die KPIs basierend auf den Optimierten CSV-Dateien|
|wait_for_calculate_kpis            |Ein Dummy Operator der wartet bis die Berechung der KPIs abgeschlossen ist.|
|get_calculated_kpis                |Der Airflow Container lädt die KPIs im CSV-Format herunter|
|create_final_excel_kpis            |Erstellt basierend auf der Heruntergeladenen KPIs mit Pandas und openpyxl eine Excel Datei|


## Bekannte Probleme
Die HIVE Tabelle ist leer, dieser Fehler ist am Ende der Entwicklung aufgetaucht.
Dieser Fehler konnte nicht so einfach behoben werden.
Da keine SQL-Abfragen gemacht werden, ist dies auch nicht besonders schlimm und schränkt somit nicht das Programm ein.
