# Databricks notebook source
# MAGIC %md
# MAGIC ## Conferindo se os dados foram montados e se temos acesso a pasta inbound  

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Lendo os dados na camada inbound

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
# MAGIC val dados = spark.read.json(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removendo Colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val dados_anuncio = dados.drop("imagens", "usuario")
# MAGIC display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Criando uma coluna de identificação

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.col

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Salvando na camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

# COMMAND ----------


