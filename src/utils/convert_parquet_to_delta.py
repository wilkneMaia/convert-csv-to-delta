import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_directory_size(path: Path) -> int:
    total = 0
    for entry in path.iterdir():
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_directory_size(entry)
    return total


def sanitize_column_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\W+", "_", name).strip("_")


def log_conversion_metrics(
    original_size: int,
    compressed_size: int,
    elapsed_time: float,
    n_partitions: int,
    compression: str,
    label_from: str = "CSV",
    label_to: str = "Delta",
) -> None:
    ratio = original_size / compressed_size if compressed_size > 0 else 0.0
    logging.info(
        "Conversão concluída!\n"
        f"  - {label_from}: {original_size / (1024**2):.2f} MB\n"
        f"  - {label_to}: {compressed_size / (1024**2):.2f} MB\n"
        f"  - Tempo: {elapsed_time:.2f}s\n"
        f"  - Compressão: {ratio:.1f}x\n"
        f"  - Partições: {n_partitions}\n"
        f"  - Algoritmo: {compression}"
    )


def convert_csv_to_delta(
    csv_file_path: str,
    delta_table_dir: str,
    compression: str = "snappy",
    overwrite: bool = True,
    n_partitions: int = 1,
) -> float | None:
    spark: SparkSession | None = None
    csv_path = Path(csv_file_path)
    delta_path = Path(delta_table_dir)

    try:
        if not csv_path.exists():
            logging.error("Arquivo CSV não encontrado: %s", csv_file_path)
            return None

        delta_path.parent.mkdir(parents=True, exist_ok=True)

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

        df = spark.read.csv(str(csv_path), header=True, inferSchema=True)

        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, sanitize_column_name(col_name))

        start_time = time.time()
        logging.info("Iniciando conversão para Delta Lake...")

        writer = df.repartition(n_partitions).write
        if overwrite:
            writer.mode("overwrite")

        writer.format("delta").option("compression", compression).save(
            str(delta_path)
        )  # ✅ Correto

        elapsed_time = time.time() - start_time

        original_size = csv_path.stat().st_size
        compressed_size = get_directory_size(delta_path)

        log_conversion_metrics(
            original_size=original_size,
            compressed_size=compressed_size,
            elapsed_time=elapsed_time,
            n_partitions=n_partitions,
            compression=compression,
            label_to="Delta",
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
        if spark:
            spark.stop()
