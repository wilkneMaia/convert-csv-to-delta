import logging
import os
import re
import time
import unicodedata

from pyspark.sql import SparkSession


def convert_csv_to_delta(
    csv_file_path: str,
    delta_table_path: str | None = None,
    overwrite: bool = True,
    n_partitions: int = 1,
    compression: str = "snappy",
) -> float | None:
    """
    Converte CSV para Delta Lake mantendo nome original com extensão .parquet.

    Args:
        csv_file_path (str): Caminho completo do arquivo CSV de entrada
        delta_table_path (str, optional): Caminho de saída. Se não informado, será gerado no mesmo diretório do CSV com extensão .parquet
        overwrite (bool): Sobrescrever tabela existente (padrão: True)
        n_partitions (int): Número de partições (padrão: 1)
        compression (str): Tipo de compressão (snappy, gzip, etc) (padrão: snappy)
    """
    spark: SparkSession | None = None

    try:
        # Gerar caminho de saída automático se não informado
        if delta_table_path is None:
            base_dir = os.path.dirname(csv_file_path)
            csv_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            delta_table_path = os.path.join(base_dir, f"{csv_name}.parquet")

        # Preparação de diretórios
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

        # Configuração do Spark
        spark = (
            SparkSession.builder.appName("CSV to Delta Converter")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1")
            .config("spark.sql.parquet.compression.codec", compression.lower())
            .getOrCreate()
        )

        # Leitura do CSV
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

        # Sanitização de colunas em lote
        sanitized_cols = [
            re.sub(
                r"\W+",
                "_",
                unicodedata.normalize("NFKD", c)
                .encode("ASCII", "ignore")
                .decode("ASCII"),
            ).strip("_")
            for c in df.columns
        ]
        df = df.toDF(*sanitized_cols)

        # Escrita com compressão configurável
        start_time = time.time()
        logging.info(f"Iniciando conversão para: {delta_table_path}")

        writer = (
            df.repartition(n_partitions)
            .write.format("delta")
            .option("compression", compression.lower())
        )
        if overwrite:
            writer.mode("overwrite")

        writer.save(delta_table_path)

        # Cálculo de métricas
        end_time = time.time()
        elapsed_time = end_time - start_time
        original_size = os.path.getsize(csv_file_path)
        compressed_size = sum(
            f.stat().st_size for f in os.scandir(delta_table_path) if f.is_file()
        )

        logging.info(
            "Conversão concluída!\n"
            f"CSV: {original_size / 1e6:.2f} MB → Delta: {compressed_size / 1e6:.2f} MB\n"
            f"Tempo: {elapsed_time:.2f}s | Compressão: {compression_ratio(original_size, compressed_size):.1f}x"
        )
        return elapsed_time

    except PermissionError as e:
        logging.error(f"Erro de permissão: {e!s}")
        return None
    except Exception as e:
        logging.error(f"Erro na conversão: {e!s}", exc_info=True)
        return None
    finally:
        if spark:
            spark.stop()


def compression_ratio(original: int, compressed: int) -> float:
    return original / compressed if compressed > 0 else 0.0
