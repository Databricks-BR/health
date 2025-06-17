# Databricks notebook source
# MAGIC %md 
# MAGIC Você pode encontrar o código completo para esse acelerador de soluções no link a seguir: https://github.com/databricks-industry-solutions/pixels.

# COMMAND ----------

# MAGIC %md 
# MAGIC %md
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/main/image/logo_dicom.png" width="250px">
# MAGIC
# MAGIC ## DICOM browser
# MAGIC
# MAGIC Visualização de imagens no formato DICOM em um Notebook Databricks.

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------

path,table,volume,write_mode = init_widgets()

# COMMAND ----------

# DBTITLE 1,Retrieve DICOM image entries indexed by the catalog and generate browser images
from dbx.pixels import Catalog
from dbx.pixels.dicom import DicomPlot

dcm_df_filtered = Catalog(spark, table=table, volume=volume).load().filter('meta:img_max < 1000 and lower(extension) = "dcm"').limit(1000)
DicomPlot(dcm_df_filtered).display()
