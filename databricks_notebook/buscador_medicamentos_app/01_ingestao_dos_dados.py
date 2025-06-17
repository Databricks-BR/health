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
import pandas as pd
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos dados

# COMMAND ----------

# DBTITLE 1,Gera dados sintéticos da farmácia
# 5 unidades de farmácias em Sao Paulo, cada uma com um número de filial
filiais = [
    {
        "filial": "001",
        "endereco": "Av. Paulista, 1000, Bela Vista, Sao Paulo, SP",
        "latitude": -23.564, 
        "longitude": -46.654
    },
    {
        "filial": "002",
        "endereco": "Rua dos Pinheiros, 200, Pinheiros, Sao Paulo, SP",
        "latitude": -23.561,
        "longitude": -46.689
    },
    {
        "filial": "003",
        "endereco": "Av. Domingos de Morais, 1500, Vila Mariana, Sao Paulo, SP",
        "latitude": -23.589,
        "longitude": -46.639
    },
    {
        "filial": "004",
        "endereco": "Av. Ibirapuera, 3000, Moema, Sao Paulo, SP",
        "latitude": -23.604,
        "longitude": -46.673
    },
    {
        "filial": "005",
        "endereco": "Rua Funchal, 500, Brooklin, Sao Paulo, SP",
        "latitude": -23.598,
        "longitude": -46.685
    }
]

# 20 remédios comuns
remedios = [
    "Dipirona", "Paracetamol", "Ibuprofeno", "Amoxicilina", "Losartana",
    "Omeprazol", "Metformina", "Cetirizina", "Ranitidina", "Cloridrato de Fluoxetina",
    "Sinvastatina", "Atenolol", "Enalapril", "Azitromicina", "Dexametasona",
    "Prednisona", "Levotiroxina", "Clonazepam", "Hidroclorotiazida", "Lorazepam",
    "Zinnat Axetilcefuroxima 500mg 14 comprimidos", "Zinnat Axetilcefuroxima 250mg/5ml Pó para Suspensão Oral 50ml + Copo Dosador + Seringa Dosadora", "Zinnat Axetilcefuroxima 250mg Pó para Suspensão Oral 20 sachês"
]

# Atribuir conjuntos de remédios para cada filial
disponibilidade = {
    "001": remedios[:10],
    "002": remedios[5:15],
    "003": remedios[10:],
    "004": remedios[:5] + remedios[10:15],
    "005": remedios[5:10] + remedios[15:]
}

# Gerar dados sintéticos
linhas = []
for filial in filiais:
    for rem in remedios:
        if rem in disponibilidade[filial["filial"]]:
            stock = random.randint(10, 100)  # Disponível, estoque aleatório
        else:
            stock = 0  # não disponível
        linhas.append({
            "farmacia": "Farmacia BemEstar",
            "filial": filial["filial"],
            "endereco": filial["endereco"],
            "cidade": "Sao Paulo",
            "estado": "SP",
            "latitude": filial["latitude"],
            "longitude": filial["longitude"],
            "remedio": rem,
            "estoque": stock
        })

# Criar o DataFrame
synthetic_df = pd.DataFrame(linhas)


# COMMAND ----------

# DBTITLE 1,Visualiza a estrutura dos dados
display(synthetic_df)

# COMMAND ----------

# DBTITLE 1,Salva tabela de estoque farmacêutico
# Primeiro, crie um catalógo com o nome 'demo_health', caso ainda não exista, ou outro nome de preferência
# O comando a seguir cria o schema, caso ainda não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS demo_health.buscador_medicamento")

# Salva o DataFrame como tabela
spark.createDataFrame(synthetic_df).write.mode("overwrite").saveAsTable("demo_health.buscador_medicamento.estoque_filial")

# COMMAND ----------

# MAGIC %md
# MAGIC Agora, execute o segundo notebook, em que realizaremos a implementação do App: [Deploy do App](https://e2-demo-field-eng.cloud.databricks.com/editor/notebooks/3054820919572927?o=1444828305810485).
