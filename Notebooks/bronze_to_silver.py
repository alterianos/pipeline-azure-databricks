# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo os dados da camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
# MAGIC val df = spark.read.format("delta").load(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transformando os campos do json  em colunas 

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*", "anuncio.endereco.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC display(dados_detalhados)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
# MAGIC display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Salvando na Silver

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
# MAGIC df_silver.write.format("delta").mode(SaveMode.Overwrite).save(path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


