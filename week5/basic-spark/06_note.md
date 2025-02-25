## command of executing 06_spark_sql.py
* need to create workers for executing jobs 
```bash
URL="spark://de-zoomcamp.us-east5-c.c.zoomcamp-hw4-shawn.internal:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
    --input_green=data/pq/green/2021/*/ \
    --input_yellow=data/pq/yellow/2021/*/ \
    --output=data/report-2021
```

    
## args of using dataproc in google cloud
```bash
    --input_green=gs://de-zoomcamp-week5/pq/green/2020/*/ \
    --input_yellow=gs://de-zoomcamp-week5/pq/yellow/2020/*/ \
    --output=gs://de-zoomcamp-week5/report-2020
```

## using dataproc api
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=dee-zoomcamp-cluster \
    --region=us-east5 \
    gs://de-zoomcamp-week5/code/06_spark_sql.py \
    -- \
    --input_green=gs://de-zoomcamp-week5/pq/green/2021/*/ \
    --input_yellow=gs://de-zoomcamp-week5/pq/yellow/2021/*/ \
    --output=gs://de-zoomcamp-week5/report-2021
```

## spark using bigquery
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=dee-zoomcamp-cluster \
    --region=us-east5 \
    gs://de-zoomcamp-week5/code/06_spark_sql_big_query.py \
    -- \
    --input_green=gs://de-zoomcamp-week5/pq/green/2021/*/ \
    --input_yellow=gs://de-zoomcamp-week5/pq/yellow/2021/*/ \
    --output=Prod.reports-2021
```
