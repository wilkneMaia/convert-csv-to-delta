import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "../data/csv")
PARQUET_DIR = os.path.join(BASE_DIR, "../data/parquet")
DELTA_DIR = os.path.join(BASE_DIR, "../data/delta")

CSV_PATH = os.path.join(BASE_DIR, "../data/csv/central_west.csv")
PARQUET_PATH = os.path.join(BASE_DIR, "../data/parquet/central_west.parquet")
