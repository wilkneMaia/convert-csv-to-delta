import logging

from utils.config import CSV_PATH, DELTA_DIR
from utils.convert_parquet_to_delta import convert_csv_to_delta

logging.basicConfig(level=logging.INFO)


def main():
    try:

        logging.info("Iniciando o processo de conversão de CSV para Delta")

        convert_csv_to_delta(
            csv_file_path=CSV_PATH,
            delta_table_dir=DELTA_DIR,
            compression="snappy",
            overwrite=True,
            n_partitions=1,
        )
    except ImportError:
        print("Erro ao importar o módulo de logging. Verifique se ele está instalado.")
        return
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return


if __name__ == "__main__":
    main()
