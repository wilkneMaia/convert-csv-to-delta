import logging
import os
import sys
import time

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Caminhos relativos para os arquivos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "../data/central_west.csv")
PARQUET_PATH = os.path.join(BASE_DIR, "../data/central_west.parquet")


def get_directory_size(path: str) -> int:
    """Calcula o tamanho total de um diretório (recursivamente)."""
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_directory_size(entry.path)
    return total


def convert_csv_to_parquet(
    csv_file_path: str,
    parquet_file_path: str,
    compression: str = "snappy",
    overwrite: bool = True,
    n_partitions: int = 1,
) -> float | None:
    """
    Converte um arquivo CSV em Parquet com compressão usando PySpark.

    Args:
        csv_file_path (str): Caminho do arquivo CSV.
        parquet_file_path (str): Caminho de saída do Parquet.
        compression (str): Tipo de compressão (snappy, gzip, etc).
        overwrite (bool): Sobrescrever arquivo existente.
        n_partitions (int): Número de partições de saída.

    Returns:
        Optional[float]: Tempo de execução em segundos ou None em caso de falha.
    """
    spark: SparkSession | None = None

    try:
        # Validação inicial
        if not os.path.exists(csv_file_path):
            logging.error("Arquivo CSV não encontrado: %s", csv_file_path)
            return None

        # Preparação de diretórios
        parquet_dir = os.path.dirname(parquet_file_path)
        os.makedirs(parquet_dir, exist_ok=True)

        # Configuração do Spark
        spark = (
            SparkSession.builder.appName("CSV to Parquet")
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .getOrCreate()
        )

        # Leitura do CSV
        logging.info("Iniciando leitura do CSV: %s", csv_file_path)
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

        # Processamento
        start_time = time.time()
        logging.info("Iniciando conversão para Parquet...")

        writer = df.repartition(n_partitions).write
        if overwrite:
            writer = writer.mode("overwrite")

        writer.option("compression", compression).parquet(parquet_file_path)

        # Cálculo de métricas
        end_time = time.time()
        elapsed_time = end_time - start_time

        original_size = os.path.getsize(csv_file_path)
        compressed_size = (
            get_directory_size(parquet_file_path)
            if os.path.isdir(parquet_file_path)
            else os.path.getsize(parquet_file_path)
        )

        if compressed_size <= 0:
            logging.error("Falha ao calcular tamanho do Parquet")
            return None

        compression_ratio = original_size / compressed_size

        # Log de métricas
        logging.info(
            "Conversão concluída com sucesso!\n"
            "Métricas:\n"
            f"  - Tamanho CSV: {original_size / (1024**2):.2f} MB\n"
            f"  - Tamanho Parquet: {compressed_size / (1024**2):.2f} MB\n"
            f"  - Tempo total: {elapsed_time:.2f} segundos\n"
            f"  - Taxa de compressão: {compression_ratio:.1f}x"
            f"  - Partições: {n_partitions}"
            f"  - Compressão: {compression}"
        )

        return elapsed_time

    except PermissionError as e:
        logging.error("Erro de permissão: %s", str(e))
        sys.exit(1)
    except Py4JJavaError as e:
        logging.error("Erro na JVM/Spark: %s", str(e.java_exception))
        return None
    except Exception as e:
        logging.error("Erro não esperado: %s", str(e), exc_info=True)
        return None
    finally:
        if spark is not None:
            spark.stop()
            logging.info("SparkSession finalizada com sucesso")


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
