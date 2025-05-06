import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import SparkSession

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


#  Calculo do tamanho do diretorio
def get_directory_size(path: Path) -> int:
    """Calcula o tamanho total de um diretório (recursivamente)."""
    total = 0
    for entry in path.iterdir():
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_directory_size(entry)
    return total


#  Remove acentos e caracteres especiais, substituindo por underscore.
def sanitize_column_name(name: str) -> str:
    """Remove acentos e caracteres especiais, substituindo por underscore."""
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\W+", "_", name).strip("_")


def log_conversion_metrics(
    original_size: int,
    compressed_size: int,
    elapsed_time: float,
    n_partitions: int,
    compression: str,
    label_from: str = "CSV",
    label_to: str = "Parquet",
) -> None:
    """
    Faz o log das métricas de conversão de arquivos.
    """
    compression_ratio = original_size / compressed_size if compressed_size > 0 else 0.0

    logging.info(
        "Conversão concluída com sucesso!\n"
        "Métricas:\n"
        f"  - Tamanho {label_from}: {original_size / (1024**2):.2f} MB\n"
        f"  - Tamanho {label_to}: {compressed_size / (1024**2):.2f} MB\n"
        f"  - Tempo total: {elapsed_time:.2f} segundos\n"
        f"  - Taxa de compressão: {compression_ratio:.1f}x\n"
        f"  - Partições: {n_partitions}\n"
        f"  - Compressão: {compression}"
    )


#  Converte um arquivo CSV em Parquet com compressão usando PySpark.
def convert_csv_to_parquet(
    csv_file_path: str,
    delta_table_dir: str,
    compression: str = "snappy",
    overwrite: bool = True,
    n_partitions: int = 1,
) -> float | None:
    """
    Converte um arquivo CSV em Parquet com compressão usando PySpark.
    Renomeia as colunas para nomes compatíveis.
    """
    spark: SparkSession | None = None
    csv_path = Path(csv_file_path)
    parquet_path = Path(delta_table_dir)

    try:
        # Validação inicial
        if not csv_path.exists():
            logging.error("Arquivo CSV não encontrado: %s", csv_file_path)
            return None

        # Preparação de diretórios
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

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
        df = spark.read.csv(str(csv_path), header=True, inferSchema=True)

        # Renomear colunas
        for old_name in df.columns:
            df = df.withColumnRenamed(old_name, sanitize_column_name(old_name))

        # Processamento
        start_time = time.time()
        logging.info("Iniciando conversão para Parquet...")

        writer = df.repartition(n_partitions).write
        if overwrite:
            writer = writer.mode("overwrite")

        writer.option("compression", compression).parquet(str(parquet_path))

        # Cálculo de métricas
        end_time = time.time()
        elapsed_time = end_time - start_time

        original_size = csv_path.stat().st_size
        compressed_size = (
            get_directory_size(parquet_path)
            if parquet_path.is_dir()
            else parquet_path.stat().st_size
        )

        # Log de métricas
        log_metrics = log_conversion_metrics(
            original_size=original_size,
            compressed_size=compressed_size,
            elapsed_time=elapsed_time,
            n_partitions=n_partitions,
            compression=compression,
            label_from="CSV",
            label_to="Parquet",
        )

        return log_metrics

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
