import logging
import os
import sys
import time
import unicodedata

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_directory_size(path: str) -> int:
    """Calcula o tamanho total de um diretório (recursivamente)."""
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_directory_size(entry.path)
    return total


def sanitize_column_name(name: str) -> str:

    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
    import re

    return re.sub(r"\W+", "_", name).strip("_")


def convert_csv_to_parquet(
    csv_file_path: str,
    parquet_file_path: str,
    compression: str = "snappy",
    overwrite: bool = True,
    n_partitions: int = 1,
) -> float | None:
    """
    Converte um arquivo CSV em Parquet com compressão usando PySpark.
    Renomeia as colunas para nomes compatíveis.
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
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1")
            .getOrCreate()
        )

        # Leitura do CSV
        logging.info("Iniciando leitura do CSV: %s", csv_file_path)
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

        # Renomear colunas usando sanitize_column_name
        for old_name in df.columns:
            df = df.withColumnRenamed(old_name, sanitize_column_name(old_name))

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
            f"  - Taxa de compressão: {compression_ratio:.1f}x\n"
            f"  - Partições: {n_partitions}\n"
            f"  - Compressão: {compression}"
        )

        return elapsed_time

    except PermissionError as e:
        logging.error("Erro de permissão: %s", str(e))
        sys.exit(1)
    except PySparkException as e:
        logging.error("Erro do PySpark: %s", str(e))
        return None
    except AnalysisException as e:
        logging.error("Erro de análise no Spark: %s", str(e))
        return None
    except Exception as e:
        logging.error("Erro não esperado: %s", str(e), exc_info=True)
        return None
    finally:
        if spark is not None:
            spark.stop()
            logging.info("SparkSession finalizada com sucesso")
