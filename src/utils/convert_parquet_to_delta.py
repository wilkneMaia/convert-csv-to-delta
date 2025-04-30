import logging
import os
import sys
import time
import unicodedata

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def sanitize_column_name(name: str) -> str:
    """Remove acentos e caracteres especiais, substituindo por underscore."""
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
    import re

    return re.sub(r"\W+", "_", name).strip("_")


def convert_csv_to_delta(
    csv_file_path: str,
    delta_table_path: str,
    overwrite: bool = True,
    n_partitions: int = 1,
) -> float | None:
    """
    Converte um CSV para Delta Lake, permitindo operações CRUD.

    Args:
        csv_file_path (str): Caminho do arquivo CSV.
        delta_table_path (str): Caminho da tabela Delta.
        overwrite (bool): Sobrescrever tabela existente.
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

        # Configuração do Spark para Delta Lake
        spark = (
            SparkSession.builder.appName("CSV to Delta")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1")
            .getOrCreate()
        )

        # Leitura do CSV
        logging.info("Lendo CSV: %s", csv_file_path)
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

        # Sanitizar nomes das colunas
        for old_name in df.columns:
            new_name = sanitize_column_name(old_name)
            df = df.withColumnRenamed(old_name, new_name)

        # Escrita como Delta Lake
        start_time = time.time()
        logging.info("Convertendo para Delta Lake...")

        writer = df.repartition(n_partitions).write.format("delta")
        if overwrite:
            writer = writer.mode("overwrite")

        writer.save(delta_table_path)

        # Tempo de execução
        elapsed_time = time.time() - start_time
        logging.info("Conversão concluída em %.2f segundos.", elapsed_time)

        return elapsed_time

    except PermissionError as e:
        logging.error("Erro de permissão: %s", str(e))
        sys.exit(1)
    except (PySparkException, AnalysisException) as e:
        logging.error("Erro do Spark: %s", str(e))
        return None
    except Exception as e:
        logging.error("Erro inesperado: %s", str(e), exc_info=True)
        return None
    finally:
        if spark:
            spark.stop()
