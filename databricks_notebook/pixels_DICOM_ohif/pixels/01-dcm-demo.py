# Databricks notebook source
# MAGIC %md 
# MAGIC Você pode encontrar o código completo para esse acelerador de soluções no link a seguir: https://github.com/databricks-industry-solutions/pixels.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/main/image/logo_dicom.png" width="250px">
# MAGIC
# MAGIC # A análise de imagens DICOM deve ser simples
# MAGIC <!-- -->
# MAGIC &nbsp;
# MAGIC - Catalogue todos os seus arquivos em paralelo e escale com Spark;
# MAGIC - Spark SQL sobre Delta Lake permite análises rápidas dos metadados;
# MAGIC - Transformers Python / pandas UDFs formam blocos de construção para:
# MAGIC   - Extração de metadados e miniaturas;
# MAGIC   - Utilização de pacotes e bibliotecas C++, como: `gdcm`, `python-gdcm` e `pydicom`;
# MAGIC   - Composição e extensão simples para De-Identificação e Deep Learning.
# MAGIC <!-- -->
# MAGIC
# MAGIC O acelerador de soluções `dbx.pixels` transforma imagens DICOM em dados SQL de forma simples.

# COMMAND ----------

# DBTITLE 1,Inicia o ambiente
# MAGIC %run ./config/setup

# COMMAND ----------

# DBTITLE 1,Display dos widgets
path,table,volume,write_mode = init_widgets()

# COMMAND ----------

# DBTITLE 1,Cria catálogo e schema, caso ainda não exista
# Crie o catálogo, esquema e volume se eles não existirem
# Verifique os widgets antes de executar este comando!

init_catalog_schema_volume()

# COMMAND ----------

# MAGIC %md ## Cataloga os objetos e arquivos
# MAGIC `dbx.pixels.Catalog` analisa apenas os metadados dos arquivos.
# MAGIC A função Catalog lista recursivamente todos os arquivos, analisando o caminho e o nome do arquivo em um dataframe. Esse dataframe pode ser salvo em um arquivo 'catalog'. Esse catálogo de arquivos pode servir de base para futuras anotações.

# COMMAND ----------

from dbx.pixels import Catalog
from dbx.pixels.dicom import DicomMetaExtractor, DicomThumbnailExtractor # The Dicom transformers

# COMMAND ----------

# DBTITLE 1,Catalog files in <path>
catalog = Catalog(spark, table=table, volume=volume)
catalog_df = catalog.catalog(path=path, extractZip=True)

# COMMAND ----------

# MAGIC %md ## Extração de Metadados das imagens Dicom
# MAGIC Usando o dataframe do Catálogo, podemos agora abrir cada arquivo Dicom e extrair os metadados do cabeçalho do arquivo Dicom. Esta operação é executada em paralelo, acelerando o processamento. O `dcm_df` resultante não inclui todo o arquivo Dicom em linha. Arquivos Dicom tendem a ser maiores, então processamos os arquivos Dicom apenas por referência.
# MAGIC
# MAGIC Por baixo dos panos, usamos PyDicom e gdcm para analisar os arquivos Dicom.
# MAGIC
# MAGIC Os metadados Dicom são extraídos em uma coluna formatada como string JSON chamada `meta`.

# COMMAND ----------

meta_df = DicomMetaExtractor(catalog).transform(catalog_df)
display(meta_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração de Miniaturas das imagens Dicom
# MAGIC O transformador `DicomThumbnailExtractor` lê os dados de pixel do Dicom, plota e armazena a miniatura embutida (cerca de 45kb) com os metadados.

# COMMAND ----------

thumbnail_df = DicomThumbnailExtractor().transform(meta_df)
display(thumbnail_df)

# COMMAND ----------

# MAGIC %md ## Salva os metadados e as miniaturas

# COMMAND ----------

catalog.save(thumbnail_df, mode=write_mode)

# COMMAND ----------

# MAGIC %sql describe ${table}

# COMMAND ----------

# MAGIC %md # Analise metadados DICOM com SQL

# COMMAND ----------

# MAGIC %sql select * from ${table}

# COMMAND ----------

# DBTITLE 1,File Metadata analysis
# MAGIC %sql
# MAGIC with x as (
# MAGIC   select
# MAGIC   format_number(count(DISTINCT meta:['00100010'].Value[0].Alphabetic),0) as patient_count,
# MAGIC   format_number(count(1),0) num_dicoms,
# MAGIC   format_number(sum(length) /(1024*1024*1024), 1) as total_size_in_gb,
# MAGIC   format_number(avg(length), 0) avg_size_in_bytes
# MAGIC   from ${table} t
# MAGIC   where extension = 'dcm'
# MAGIC )
# MAGIC select patient_count, num_dicoms, total_size_in_gb, avg_size_in_bytes from x

# COMMAND ----------

# MAGIC %md ### Decodifique atributos Dicom
# MAGIC Usando os códigos do DICOM Standard Browser (https://dicom.innolitics.com/ciods) por Innolitics

# COMMAND ----------

# DBTITLE 1,Patient / Radiology Data Analysis
# MAGIC %sql
# MAGIC SELECT
# MAGIC     --rowid,
# MAGIC     meta:['00100010'].Value[0].Alphabetic patient_name, 
# MAGIC     meta:['00082218'].Value[0]['00080104'].Value[0] `Anatomic Region Sequence Attribute decoded`,
# MAGIC     meta:['0008103E'].Value[0] `Series Description Attribute`,
# MAGIC     meta:['00081030'].Value[0] `Study Description Attribute`,
# MAGIC     meta:`00540220`.Value[0].`00080104`.Value[0] `projection` -- backticks work for numeric keys
# MAGIC FROM ${table}

# COMMAND ----------

# DBTITLE 1,Query the object metadata table using the JSON notation
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   --rowid,
# MAGIC   meta:['00100010'].Value[0].Alphabetic as patient_name,  -- Medical information from the DICOM header
# MAGIC   meta:hash, meta:img_min, meta:img_max, path,            -- technical metadata
# MAGIC   meta                                                    -- DICOM header metadata as JSON
# MAGIC FROM ${table}
# MAGIC WHERE array_contains( path_tags, 'patient5397' ) -- query based on a part of the filename
# MAGIC order by patient_name

# COMMAND ----------

# MAGIC %md
# MAGIC Próximo: <a href="$./02-dcm-browser">DICOM Image Browser</a>
