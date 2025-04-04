import pandas as pd

# GCS URI of uploaded CSV
gcs_uri = "gs://zoomcamp-proj-shawn-bucket/us_accidents.csv"

# Read a sample of the file
df = pd.read_csv(gcs_uri, nrows=100)

# Show inferred schema
print("Inferred schema (first 100 rows):\n")
print(df.dtypes)
