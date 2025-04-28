import logging

from utils.config import CSV_PATH, PARQUET_PATH
from utils.convert_csv_to_parquet import convert_csv_to_parquet


def main():
    time_elapsed = convert_csv_to_parquet(
        csv_file_path=CSV_PATH,
        parquet_file_path=PARQUET_PATH,
        compression="snappy",
        n_partitions=1,
        overwrite=True,
    )

    if time_elapsed is not None:
        logging.info("Processo finalizado em %.2f segundos", time_elapsed)
    else:
        logging.error("Processo falhou")


if __name__ == "__main__":
    main()
