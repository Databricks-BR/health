# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/main/image/head_notebook.png" width="1000px">

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Descrição e Objetivos
# MAGIC
# MAGIC Este projeto tem como objetivo facilitar a busca por medicamentos em drogarias e farmácias próximas à localização atual do usuário. Através da consulta ao controle de estoque da unidade, o sistema verifica a disponibilidade do medicamento desejado, garantindo informações precisas e atualizadas para auxiliar o usuário na obtenção do medicamento de forma rápida e eficiente.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Controle de Versão do Código
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 12-JUN-2025 | Bruna Robledo<br>Luis Assunção<br>Vinicius Fialho | bruna.robledo@databricks.com<br>luis.assuncao@databricks.com<br>vinicius.fialho@databricks.com | Primeira versão  |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Descrição do Cluster
# MAGIC
# MAGIC Toda a demo pode ser executada utilizando Serverless ou um tipo de instância de sua pereferência.

# COMMAND ----------

# DBTITLE 1,Instala e atualiza as bibliotecas necessárias
# MAGIC %pip install --upgrade databricks-sdk==0.36.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configura o Cliente do Workspace no Databricks
from databricks.sdk import WorkspaceClient

app_name = "buscador-medicamentos"
# Vá na aba do Databricks SQL Warehouse, selecione o SQL Warehouse desejado, copie o ID e cole na variável abaixo
sql_warehouse_id = "4b9b953939869799"

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy do Aplicativo

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/health/refs/heads/main/image/busca_medicamento.png" width="1000px">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Para realizarmos o deploy do aplicativo, utilizaremos o **Databricks Apps**, que são aplicações fáceis de criar e usar dentro da plataforma Databricks, permitindo transformar dados e inteligência artificial em soluções práticas. Elas oferecem uma forma rápida e segura de construir ferramentas interativas para análise, visualização e automação, ajudando equipes a tomar decisões melhores com base nos dados.
# MAGIC
# MAGIC Para saber mais sobre Databricks Apps, [acesse a documentação](https://docs.databricks.com/aws/pt/dev-tools/databricks-apps).

# COMMAND ----------

# DBTITLE 1,Deploy e Gerenciamento dos Recursos do Databricks App
from databricks.sdk.service.apps import AppResource, AppResourceSqlWarehouse, AppResourceSqlWarehouseSqlWarehousePermission, AppResourceServingEndpoint, AppResourceServingEndpointServingEndpointPermission

import os

# Obtém o caminho da pasta de criação do Lakehouse App
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_folder = os.path.dirname(notebook_path)
path = (f"/Workspace{notebook_folder}/lakehouse_app")

resources = []

# Define os recursos a serem utilizados
sql_resource = AppResource(
  name="sql_warehouse",
  sql_warehouse=AppResourceSqlWarehouse(
    id=sql_warehouse_id,
    permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE
  )
)
resources.append(sql_resource)

# Checa se o Lakehouse App já existe
# Caso o App já exista, apenas será feito o deploy novamente, sem necessidade de criá-lo
if app_name in [app.name for app in w.apps.list()]:
  print(f"App {app_name} já existe")
  app = w.apps.get(app_name)

  app_deploy = w.apps.deploy_and_wait(app_name=app_name, source_code_path=path)
  print(app_deploy.status.message)
  print(app.url)

else:
  print(f"Criando Lakehouse App com o nome {app_name}, esta etapa deve demorar alguns minutos para completar")

  app_created = w.apps.create_and_wait(name=app_name, resources=resources)
  app_deploy = w.apps.deploy_and_wait(app_name=app_name, source_code_path=path)

  print(app_deploy.status.message)
  print(app_created.url)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo importante
# MAGIC Logo após criar o App, navegue na interface do Databricks para a aba Compute > Apps > Selecione o App criado > clique em Authorization > copie o valor do Service Principal > vá até o catálogo de dados criado na aba Catalog e dê permissão para o Service Principal utilizar os catálogo e schema em que armazenamos os dados.
# MAGIC
# MAGIC Após isso, será possível utilizar o aplicativo com todos os dados já integrados.
