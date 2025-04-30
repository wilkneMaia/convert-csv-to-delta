# """
# App simples PySpark: leitura de Parquet e consulta SQL

# Pré-requisitos:
# - Instale o PySpark: pip install pyspark
# - Altere o caminho do arquivo Parquet conforme necessário
# """

# from pyspark.sql import SparkSession

# # Inicializa SparkSession
# spark = SparkSession.builder.appName("ConsultaParquet").getOrCreate()
# name_column = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
# # Caminho do arquivo Parquet
# parquet_path = "/Users/wilkne/Development/parquet-query/src/data/central_west.parquet/data_1.snappy.parquet"

# # Lê o arquivo Parquet
# df = spark.read.parquet(parquet_path)

# # Cria uma view temporária para consultas SQL
# df.createOrReplaceTempView("tabela_parquet")

# # Exemplo de consulta SQL
# resultado = spark.sql("""
#     SELECT *
#     FROM tabela_parquet
#     WHERE salario >= 4000
# """)
# resultado = df.filter(df.salario >= 4000)


# # Mostra o resultado
# resultado.show(truncate=False)

# # Encerra a sessão Spark
# spark.stop()


from utils.convert_parquet_to_delta import convert_csv_to_delta


def main():
    parquet_dir = "/Users/wilkne/Development/parquet-query/src/data/csv_2"
    delta_dir = "/Users/wilkne/Development/parquet-query/src/data/delta"

    # Dicionário de colunas inválidas para renomear
    rename_columns = {}

    convert_csv_to_delta(
        csv_file_path=parquet_dir,
        delta_table_path=delta_dir,
        overwrite=True,
        n_partitions=1,
    )


if __name__ == "__main__":
    main()
