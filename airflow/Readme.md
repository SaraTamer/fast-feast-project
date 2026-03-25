# Airflow Setup Guide 

This guide explains how to run Apache Airflow locally using Docker for this project.

---

## Requirements

Make sure you have the following installed:

* Docker


## Setup Instructions

### 1. Navigate to the Airflow directory

```bash
cd airflow
```


### 2. Start Airflow services

```bash
docker-compose up airflow-init
```

```bash
docker-compose up -d
```

### 3. Open Airflow UI

Go to:

```
http://localhost:8080
```


### 4. Login Credentials

* **Username:** airflow
* **Password:** airflow




---

## Logs

Logs are stored in:

```
airflow/logs/
```

---

