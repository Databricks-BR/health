# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/main/image/head_notebook.png" width="1000px">

# COMMAND ----------

# MAGIC %md 
# MAGIC You may find this solution accelerator at https://github.com/databricks-industry-solutions/pixels.

# COMMAND ----------

# MAGIC %md
# MAGIC #### DICOM Modality
# MAGIC
# MAGIC
# MAGIC | Value | Description |
# MAGIC | -- | -- |
# MAGIC | CR | Computed Radiography (X-Ray) |
# MAGIC | CT | Computed Tomography |
# MAGIC | DX | Digital Radiography |
# MAGIC | MG | Mammography |
# MAGIC | MR | Magnetic Resonance |
# MAGIC | NM | Nuclear Medicine | 
# MAGIC | PT | Positron emission tomography (PET) |
# MAGIC | US | Ultrasound |
# MAGIC | XA | X-Ray Angiography |
# MAGIC  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------

path,table,volume,write_mode = init_widgets()

# COMMAND ----------

# MAGIC %md 
# MAGIC %md
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/main/image/logo_dicom.png" width="250px">
# MAGIC
# MAGIC ## DICOM BROWSER
# MAGIC
# MAGIC Visualização de imagens no formato DICOM no Notebook

# COMMAND ----------

# DBTITLE 1,Retrieve DICOM image entries indexed by the catalog and generate browser images
from dbx.pixels import Catalog
from dbx.pixels.dicom import DicomPlot

dcm_df_filtered = Catalog(spark, table=table, volume=volume).load().filter('meta:img_max < 1000 and lower(extension) = "dcm"').limit(1000)
DicomPlot(dcm_df_filtered).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Previous: <a href="$./01-dcm-demo">DICOM demo</a>
