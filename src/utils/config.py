import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "../data/csv")
PARQUET_DIR = os.path.join(BASE_DIR, "../data/parquet")
PARQUET_PATH = os.path.join(BASE_DIR, "../data/parquet/central_west.parquet")

CSV_PATH = os.path.join(BASE_DIR, "../data/csv/central_west.csv")
DELTA_DIR = os.path.join(BASE_DIR, "../data/delta")
DELTA_PATH = os.path.join(
    BASE_DIR,
    "../data/delta/part-00000-cc878666-2a59-48a4-a718-c35988d6558d-c000.snappy.parquet",
)
