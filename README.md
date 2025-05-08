![Comparativo CSV vs Delta Lake](comparativo_delta_linkedin.png)


# Do CSV ao Delta Lake: Armazenando e Consultando Dados Frios com Python e SQL

## 🚀 Visão Geral

Este projeto demonstra como converter grandes arquivos CSV de dados frios em tabelas Delta Lake (Parquet Snappy), otimizando o armazenamento e acelerando consultas SQL. Utilizando Python, PySpark e Delta Lake, você reduz custos, ganha performance e prepara seus dados para análise moderna.

---

## 📦 Principais Benefícios

- **Redução de espaço:** De 1.8 GB (CSV) para 216 MB (Delta Lake) - compressão de 8x+
- **Consultas SQL ultrarrápidas:** 14x mais rápido que CSV puro
- **Pronto para BI:** Integração nativa com Spark, Databricks, Athena, Power BI, etc.
- **Governança e escalabilidade:** Transações ACID, versionamento, time travel e schema evolution

---

## ⚡ Pipeline

1. **Leitura do CSV:** Inferência automática de schema
2. **Sanitização de colunas:** Padronização de nomes para compatibilidade SQL
3. **Conversão para Delta Lake:** Compressão Snappy e escrita otimizada
4. **Consulta SQL:** Filtros e agregações rápidas sobre grandes volumes de dados frios

---

## 📊 Resultados Reais

| Métrica         | CSV        | Delta Lake | Ganho      |
|-----------------|------------|------------|------------|
| Tamanho         | 1807 MB    | 216 MB     | 8.3x menor |
| Tempo de Query  | 12.4 s     | 0.86 s     | 14x mais rápido |

> **Gráfico acima:** Redução de espaço e ganho de performance ao migrar dados frios de CSV para Delta Lake.

---

## 🔍 Exemplo de Uso

### Conversão para Delta Lake

---

## 🛠️ Requisitos

- Python ^3.11
- PySpark ^3.5.5
- delta-spark ^3.3.1
- Java 8/11

---

## 📂 Estrutura do Projeto

![Comparativo CSV vs Delta Lake](/img/img_1.png)

---

## 🤝 Contribua e Compartilhe

- Teste o pipeline com seus próprios dados frios.
- Compartilhe sua experiência ou dúvidas na seção de Issues ou nos comentários do LinkedIn.
- Pull requests são bem-vindos!

---

## 📎 Referências

- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [delta-io/delta-examples](https://github.com/delta-io/delta-examples)
- [Kaggle - dataset](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region)

---

**Vamos juntos modernizar o tratamento de dados frios!** 🚀
