#!/usr/bin/env python
# coding: utf-8

# ## mailing_carga_merge
# 
# 
# 

# In[1]:


get_ipython().run_cell_magic('pyspark', '', 'import pandas as pd\r\nimport re\r\nfrom datetime import datetime\r\nfrom pyspark.sql import SparkSession\r\nfrom delta.tables import DeltaTable\r\nfrom pyspark.sql.window import Window\r\nfrom pyspark.sql.functions import *\r\nfrom pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType\r\n\r\n# Obter a data atual\r\ndata_atual = datetime.now().strftime("%Y-%m-%d")\r\n\r\n# Caminho para os arquivos xlsx\r\ncaminho = "abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/inputs/mailing/"\r\n\r\n# Carregar os arquivos xlsx como DataFrames\r\n#df_mailing_vibra = pd.read_excel(caminho + "Mailing_Tr*.xlsx")\r\ndf_mailing_vibra = pd.read_excel(caminho + "base_reativação_regiões_wa_final_25_04_2024.xlsx")\r\n#df_mailing_vibra_2 = pd.read_excel(caminho + "CanalTratado2.xlsx")\r\ndf_mailing_insights = pd.read_excel(caminho + "Mailing_insights_2024_04_12_score.xlsx")\r\ndf_contatos_quentes = pd.read_excel(caminho + "Contatos_Quentes*.xlsx")\r\ndf_contatos_whatsapp = pd.read_excel(caminho + "Contatos_*Whatsapp*.xlsx")\r\n\r\n## Filtrar contatos que o EV autorizou a atuação\r\n\r\ndf_contatos_quentes = df_contatos_quentes[df_contatos_quentes["PODEMOS ATUAR?"] == "Sim"]\r\n\r\n\r\n#df_mailing_vibra = df_mailing_vibra[df_mailing_vibra["ORIGEM"]!= "REATIVAÇÃO"]\r\n')


# In[2]:


df_mailing_vibra['CNPJ'] = df_mailing_vibra['CNPJ'].astype(int)
df_mailing_vibra['VEDI_COD_CLI'] = df_mailing_vibra['VEDI_COD_CLI'].astype(int)
df_mailing_vibra['CNPJ_BASICO'] = df_mailing_vibra['CNPJ_BASICO'].astype(int)
df_mailing_vibra['VEDI_COD_MAT'] = df_mailing_vibra['VEDI_COD_MAT'].astype(int)
# Verificar se a coluna 'VEDI_DAT_VEN' já é do tipo datetime64
if df_mailing_vibra['VEDI_DAT_VEN'].dtype != 'datetime64[ns]':
    # Converter VEDI_DAT_VEN para data
    df_mailing_vibra['VEDI_DAT_VEN'] = pd.to_datetime(df_mailing_vibra['VEDI_DAT_VEN'], unit='D', origin='1899-12-30')

df_mailing_vibra['VEDI_COD_BAS'] = df_mailing_vibra['VEDI_COD_BAS'].astype(int)

# Verificar se há valores não numéricos na coluna 'Distância'
non_numeric_values = df_mailing_vibra['Distância'].loc[~df_mailing_vibra['Distância'].apply(lambda x: isinstance(x, (int, float)))]

# Se houver valores não numéricos, você pode tratá-los de acordo com sua necessidade. Por exemplo, converter para NaN
df_mailing_vibra['Distância'] = pd.to_numeric(df_mailing_vibra['Distância'], errors='coerce')

# Agora, você pode arredondar os valores
df_mailing_vibra['Distância'] = df_mailing_vibra['Distância'].round(4)

df_mailing_vibra['VEDI_DAT_VEN'].head() 


# In[129]:


"""# Lista de colunas a serem preenchidas
colunas_para_preencher = [coluna for coluna in df_mailing_vibra.columns if df_mailing_vibra[coluna].isnull().any()]

# Função para preencher os valores nulos com base nos valores correspondentes de outras linhas
def preencher_valores_nulos(row):
    vedi_cod_cli = row['VEDI_COD_CLI']
    for coluna in colunas_para_preencher:
        if pd.isnull(row[coluna]):
            valores_nao_nulos = df_mailing_vibra[(df_mailing_vibra['VEDI_COD_CLI'] == vedi_cod_cli) & (~df_mailing_vibra[coluna].isnull())][coluna]
            if not valores_nao_nulos.empty:
                row[coluna] = valores_nao_nulos.iloc[0]
    return row

# Aplicar a função a cada linha do DataFrame
df_mailing_vibra = df_mailing_vibra.apply(preencher_valores_nulos, axis=1)
df_mailing_vibra = df_mailing_vibra.applymap(lambda x: '{:.4f}'.format(x) if isinstance(x, float) else x)

# Agora o DataFrame df_mailing_vibra deve ter os valores nulos preenchidos com base nos valores correspondentes de outras linhas

#df_mailing_vibra.head(50)"""


# In[3]:


# Unir os DataFrames df_mailing_vibra_1 e df_mailing_vibra_2
#df_mailing_vibra = pd.concat([df_mailing_vibra_1, df_mailing_vibra_2])

# Renomear as colunas de df_mailing_insights para que sejam iguais antes do join
df_mailing_insights.rename(columns={"Sap Code": "VEDI_COD_CLI"}, inplace=True)

# Realizar o inner join entre df_mailing_vibra e df_mailing_insights
df_merged = pd.merge(df_mailing_vibra, df_mailing_insights, on="VEDI_COD_CLI", how="left")

# Renomear as colunas de df_mailing_insights para que sejam iguais antes do join
df_contatos_quentes.rename(columns={"Código SAP": "VEDI_COD_CLI"}, inplace=True)
# Renomear as colunas de df_mailing_insights para que sejam iguais antes do join
df_contatos_whatsapp.rename(columns={"Código SAP": "VEDI_COD_CLI"}, inplace=True)


# In[4]:


df_vibra = []
df_merged = pd.merge(df_merged, df_contatos_quentes[["VEDI_COD_CLI", "TELEFONE CONTATO 2"]], on="VEDI_COD_CLI", how="left")

df_merged = pd.merge(df_merged, df_contatos_whatsapp[["VEDI_COD_CLI", "WhatsApp Telefone"]], on="VEDI_COD_CLI", how="left")

df_vibra = df_merged.copy()


df_vibra.columns


# In[6]:


for col in ["TELEFONE CONTATO 2", "WhatsApp Telefone"]:
    df_vibra[col] = df_vibra[col].astype(str)
    df_vibra[col] = df_vibra[col].replace("nan", "")

df_vibra['contatos_concatenados'] = df_vibra[["TELEFONE CONTATO 2", "WhatsApp Telefone"] + [str(i) for i in range(1, 11)]].apply(lambda x: " || ".join(x.dropna()), axis=1)

#+ [f"{i}" for i in range(1, 11)]].apply(lambda x: "__".join(x.dropna()), axis=1)

#df_vibra.to_excel("abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/teste.xlsx", index=False)


# In[7]:


def find_mobile(value):
    # encontrar números de telefone móvel válidos no Brasil
    #pattern = r'(?:\+?55)? ?(?:\(?([1-9]{2})\)? ?)?(?:9[6-9])\d{3}[- ]?\d{4}'
    pattern = r'(?:\+?55 ?)?(?:\(?([1-9]{2})\)? ?)?(?:9\d{4}|\d{2}\d{5}|\d{5})[- ]?\d{4}'
    match = re.search(pattern, str(value))
    if match:
        # Formatar o número de telefone móvel encontrado no formato robbu
        formatted_number = "55" + re.sub(r'\D', '', match.group())
        return formatted_number
    else:
        return None

def find_fixo(value):
    # Encontrar números de telefone fixo válidos no Brasil
    pattern = r'(?:\+?55)? ?(?:\(?([1-9]{2})\)? ?)?(?:[2-5])(?:\d{3,4}[- ]?\d{4})'
    match = re.search(pattern, str(value))
    if match:
        # Formatar o número de telefone fixo encontrado
        formatted_number = "55" + re.sub(r'\D', '', match.group())
        return formatted_number
    else:
        return None



#df_merged['contatos_concatenados'] = df_merged[[str(i) for i in range(1, 11)]].apply(lambda x: "__".join(x.dropna()), axis=1)

df_vibra['contato_mobile'] = df_vibra['contatos_concatenados'].apply(find_mobile)
df_vibra['contato_fixo'] = df_vibra['contatos_concatenados'].apply(find_fixo)

# Selecionar os campos desejados
df_pipefy_todo = df_vibra[["Nome Cliente", "Grupo Cliente", "AECO", "Segmento", "LBR Médio Mensal", "Volume Médio Mensal", "ZV_BR", "TITULAR", "H_N3", "H_N2", "Canal Fonte", "VEDI_COD_CLI", "CLIE_NOM_MUNICIPIO", "CLIE_NOM_ESTADO", "CNPJ_BASICO", "CEP", "BAIRRO", "RUA", "NUM", "COMPLEMENTO", "VEDI_COD_MAT", "VEDI_DSC_MAT", "VEDI_DAT_VEN", "VEDI_VOL", "VEDI_DSC_ICT", "Produto_D", "VEDI_COD_BAS", "Base", "Lista de Preço", "Exposição Total", "% Exposição", "Limite de credito", "Limite de Crédito", "Distância", "Market", "SLA DE ENTREGA", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "Novo Preço Final (R$/l)", "Dias De Prazo", "GRUPO_CNPJ_BASICO", "Novo Preço Final Limite", "CNPJ_x", "contato_mobile", "contato_fixo", "flag_mkt", "Fonte", "Território", "Grupo", "Cluster", "Terça Feira - 16/04", "Quarta Feira - 17/04", "Quinta Feira - 18/04", "Sexta Feira - 19/04", "Segunda Feira - 22/04", "Canal"]]

# Renomear os campos "contato_mobile" e "contato_fixo"
df_pipefy_todo = df_pipefy_todo.rename(columns={"contato_mobile": "fone_1", "contato_fixo": "fone_2", "CNPJ_x": "CNPJ"})

# Substituir os valores da coluna 'canal' para 'WhatsApp' onde 'fone_1' estiver preenchido
#df_pipefy_todo.loc[df_pipefy_todo['fone_1'].notnull(), 'Canal'] = 'WhatsApp'


# Separar bases WPP e DIs:

df_pipefy_todo['Novo Preço Final (R$/l)'] = df_pipefy_todo['Novo Preço Final (R$/l)'].astype(float).round(4)
df_pipefy_todo['Novo Preço Final Limite'] = df_pipefy_todo['Novo Preço Final Limite'].astype(float).round(4)
df_pipefy_todo['fone_1'] = df_pipefy_todo['fone_1'].astype(str)
df_pipefy_todo['fone_2'] = df_pipefy_todo['fone_2'].astype(str)



# Adicionando filtro para excluir linhas onde a coluna "ORIGEM" é igual a "REATIVAÇÃO"
df_pipefy_dis = df_pipefy_todo[(df_pipefy_todo["Canal"] == "DIS")]
df_pipefy_wpp = df_pipefy_todo[(df_pipefy_todo["Canal"] == "WhatsApp")]



# Aplicar condição para preencher o campo "Canal" com "DIS" se o campo "fone_1" estiver vazio, caso contrário, preencher com "WhatsApp"
df_pipefy_todo['Canal'] = df_pipefy_wpp.apply(lambda x: 'DIS' if pd.isnull(x['fone_1']) or x['fone_1'] == '' else 'WhatsApp', axis=1)
df_pipefy_wpp = df_pipefy_todo.copy()

#df_pipefy_todo['Novo Preço Final (R$/l)'].head()
#df_pipefy_todo['Novo Preço Final Limite'].head()

#df_pipefy_todo.to_excel("abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/pipefy_todo.xlsx", index=False)


# In[35]:


# Primeiro, vamos agrupar os dados pelo Grupo Cliente, CNPJ_BASICO e VEDI_DSC_MAT e encontrar os menores valores de "Novo Preço Final (R$/l)" para o agrupamento:

indices_min_preco = df_pipefy_wpp.groupby(['Grupo Cliente', 'CNPJ_BASICO', 'VEDI_DSC_MAT','fone_1'])['Novo Preço Final Limite'].idxmin()

# Agora, selecionamos as linhas correspondentes aos índices encontrados
df_menor_preco = df_pipefy_wpp.loc[indices_min_preco]
df_rb = df_menor_preco.drop(columns=[str(i) for i in range(1, 11)])




# In[38]:


# Exportar o DataFrame para um arquivo Excel
#df_pivotado.to_excel("abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/vro/files/mailing/mailing_vibra/teste.xlsx", index=False)
campos_endereco = ["CEP", "BAIRRO", "RUA", "NUM", "COMPLEMENTO", "CLIE_NOM_MUNICIPIO", "CLIE_NOM_ESTADO"]

# Concatenar campos de endereço
df_rb["endereco_concatenado"] = df_rb[campos_endereco].apply(lambda x: ", ".join(x.dropna().astype(str).unique()), axis=1)

# Agrupar e concatenar os valores únicos
df_agrupado = df_rb.groupby(["Nome Cliente","Grupo Cliente", "CNPJ_BASICO"]).agg({
    "VEDI_COD_CLI": lambda x: list(x.unique()),
    "fone_1": lambda x: list(x.unique()),
     "VEDI_DSC_MAT": lambda x: list(x.unique()),
     "endereco_concatenado": lambda x: list(x.unique()),
     "Novo Preço Final (R$/l)": lambda x: list(x.unique()),
     "Novo Preço Final Limite": lambda x: list(x.unique()),
     "SLA DE ENTREGA": lambda x: x.tolist(),
     "VEDI_DSC_ICT": lambda x: x.tolist(),
     "Dias De Prazo": lambda x: x.tolist(),
     "Base": lambda x: x.tolist()
}).reset_index()

df_rb_pvt = df_agrupado.copy()
# Dividir os campos concatenados ou em arrays em colunas diferentes numeradas
for coluna in ["VEDI_COD_CLI", "fone_1", "VEDI_DSC_MAT","Base", "endereco_concatenado","Novo Preço Final (R$/l)","Novo Preço Final Limite","SLA DE ENTREGA","VEDI_DSC_ICT","Dias De Prazo"]:
    coluna_nova = []
    for i in range(1, 4):
        coluna_nova.append(f"{coluna}_{i}")
        df_rb_pvt[f"{coluna}_{i}"] = df_rb_pvt[coluna].apply(lambda x: x[i-1] if len(x) >= i else None)

colunas_para_dropar = ["VEDI_COD_CLI", "fone_1", "VEDI_DSC_MAT", 
                       "endereco_concatenado", "Novo Preço Final (R$/l)", 
                       "SLA DE ENTREGA", "VEDI_DSC_ICT", "Dias De Prazo","Base","Novo Preço Final Limite"]

df_rb_pvt.drop(columns=colunas_para_dropar, inplace=True)

df_rb_pvt.head(40)
#df_rb_pvt.to_excel("abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/vro/files/mailing/mailing_vibra/teste.xlsx", index=False)
df_rb_pvt = df_rb_pvt[df_rb_pvt['fone_1_1'] != 'None']

df_rb_pvt.count()


# In[39]:


ofertas = []  # Lista para armazenar as ofertas para cada cliente

# Função para substituir quebras de linha na variável "oferta_cliente"
def replace_newlines_in_oferta(oferta_cliente):
    return oferta_cliente.replace('\n', "\\n ")

# Iterar sobre cada linha do DataFrame
for index, row in df_rb_pvt.iterrows():
    # Inicializar a string para armazenar as ofertas para este cliente
    oferta_cliente = ""
    
    # Verificar se há informações para o produto 1
    if not pd.isnull(row["VEDI_DSC_MAT_1"]):
        oferta = f"Oferta 1\nBase: {row['Base_1']}\nCNPJ_RAIZ: {row['CNPJ_BASICO']}\nProduto: {row['VEDI_DSC_MAT_1']}\nPreço por litro: {row['Novo Preço Final (R$/l)_1']}\nPreço mín por litro: {row['Novo Preço Final Limite_1']}\nPreço máx por litro:\nModalidade de Entrega: {row['VEDI_DSC_ICT_1']}\nPrazo para pagamento: {row['Dias De Prazo_1']}\nPrazo de entrega: {row['SLA DE ENTREGA_1']}\nEndereço de entrega do Ponto de Consumo: {row['endereco_concatenado_1']}\n"
        oferta_cliente += oferta

    # Verificar se há informações para o produto 2
    if not pd.isnull(row["VEDI_DSC_MAT_2"]) and str(row["VEDI_COD_CLI_2"]) != "nan":
        oferta = f"Oferta 2\nBase: {row['Base_2']}\nCNPJ_RAIZ: {row['CNPJ_BASICO']}\nProduto: {row['VEDI_DSC_MAT_2']}\nPreço por litro: {row['Novo Preço Final (R$/l)_2']}\nPreço mín por litro: {row['Novo Preço Final Limite_2']}\nPreço máx por litro:\nModalidade de Entrega: {row['VEDI_DSC_ICT_2']}\nPrazo para pagamento: {row['Dias De Prazo_2']}\nPrazo de entrega: {row['SLA DE ENTREGA_2']}\nEndereço de entrega do Ponto de Consumo: {row['endereco_concatenado_2']}\n"
        oferta_cliente += oferta

    # Verificar se há informações para o produto 3
    if not pd.isnull(row["VEDI_DSC_MAT_3"]) and str(row["VEDI_COD_CLI_3"]) != "nan":
        oferta = f"Oferta 3\nBase: {row['Base_3']}\nCNPJ_RAIZ: {row['CNPJ_BASICO']}\nProduto: {row['VEDI_DSC_MAT_3']}\nPreço por litro: {row['Novo Preço Final (R$/l)_3']}\nPreço mín por litro: {row['Novo Preço Final Limite_3']}\nPreço máx por litro:\nModalidade de Entrega: {row['VEDI_DSC_ICT_3']}\nPrazo para pagamento: {row['Dias De Prazo_3']}\nPrazo de entrega: {row['SLA DE ENTREGA_3']}\nEndereço de entrega do Ponto de Consumo: {row['endereco_concatenado_3']}\n"
        oferta_cliente += oferta

    
    # Aplicar a substituição de quebras de linha na variável "oferta_cliente"
    oferta_cliente = replace_newlines_in_oferta(oferta_cliente)
    
    # Adicionar as ofertas para este cliente à lista
    ofertas.append(oferta_cliente)



# In[40]:


df_final_robbu = pd.DataFrame()

# Preencher os campos


df_final_robbu["VALOR_DO_REGISTRO"] = df_rb_pvt["fone_1_1"]
df_final_robbu["MENSAGEM"] = ""
df_final_robbu["NOME_CLIENTE"] = ""
df_final_robbu["CPFCNPJ"] = ""  #O campo está indo vazio para o Robbu não agrupar os contatos de mesmo CNPJ BASICO
df_final_robbu["CODCLIENTE"] = ""
df_final_robbu["TAG"] = ""

# Preencher os campos CORINGA1, CORINGA2 e CORINGA3
for i in range(1, 4):
    coringa_col = f"CORINGA{i}"
    df_final_robbu[coringa_col] = (
        "*" + df_rb_pvt[f"VEDI_DSC_MAT_{i}"] + "* - A partir de *R$" +
        df_rb_pvt[f"Novo Preço Final Limite_{i}"].astype(str) + "*/litro"
    )

# Preencher o campo CORINGA4
df_rb_pvt["VEDI_COD_CLI_1"] = df_rb_pvt["VEDI_COD_CLI_1"].astype(str)
df_rb_pvt["VEDI_COD_CLI_2"] = df_rb_pvt["VEDI_COD_CLI_2"].astype(str)
df_rb_pvt["VEDI_COD_CLI_3"] = df_rb_pvt["VEDI_COD_CLI_3"].astype(str)

# Preencher o campo CORINGA4
coringa4 = []

# Iterar sobre cada linha do DataFrame
for index, row in df_rb_pvt.iterrows():
    codigos = []  # Lista para armazenar os códigos para este cliente
    
    # Verificar se há informações válidas para o código do produto 1
    if pd.notnull(row["VEDI_COD_CLI_1"]) and row["VEDI_COD_CLI_1"] != "None" and row["VEDI_COD_CLI_1"] != "nan":
        codigos.append(str(row["VEDI_COD_CLI_1"]))

    # Verificar se há informações válidas para o código do produto 2
    if pd.notnull(row["VEDI_COD_CLI_2"]) and row["VEDI_COD_CLI_2"] != "None" and row["VEDI_COD_CLI_2"] != "nan":
        # Obter o código como uma string
        codigo = str(row["VEDI_COD_CLI_2"])

        # Separar o código pelo ponto decimal
        partes = codigo.split(".")

        # Adicionar apenas a parte inteira do código à lista
        codigos.append(partes[0])

    # Verificar se há informações válidas para o código do produto 3
    if pd.notnull(row["VEDI_COD_CLI_3"]) and row["VEDI_COD_CLI_3"] != "None" and row["VEDI_COD_CLI_3"] != "nan":
        # Obter o código como uma string
        codigo = str(row["VEDI_COD_CLI_3"])

        # Separar o código pelo ponto decimal
        partes = codigo.split(".")

        # Adicionar apenas a parte inteira do código à lista
        codigos.append(partes[0])
    
    # Verificar se há mais de um código antes de concatená-los
    if len(codigos) > 0:
        codigos_concatenados = " || ".join(codigos)
    else:
        codigos_concatenados = ""  # Se não houver códigos, marcá-lo como vazio
    
    # Adicionar os códigos para este cliente à lista
    coringa4.append(codigos_concatenados)

df_final_robbu["CORINGA4"] = coringa4

df_final_robbu["CORINGA5"] = ofertas

df_final_robbu["PRIORIDADE"] = "ALTA"

df_final_robbu.insert(0, "TIPO_DE_REGISTRO", "TELEFONE")


#DIS: Substituir os valores da coluna "10" pela coluna "fone_2" se "fone_2" não for nulo
df_pipefy_dis.loc[df_pipefy_dis['fone_2'].notnull(), '10'] = df_pipefy_dis['fone_2']


# Substituir os valores da coluna "10" pela coluna "fone_1" se "fone_1" não for nulo
df_pipefy_wpp.loc[df_pipefy_wpp['fone_1'].notnull(), '10'] = df_pipefy_wpp['fone_1'].copy()


df_final_robbu['VALOR_DO_REGISTRO'] = df_final_robbu['VALOR_DO_REGISTRO'].astype(int)
#df_final_robbu.head(10)

df_final_robbu.count()


# In[41]:


# Criar DataFrames filtrados para cada quantidade de ofertas

df_carga_robbu_1 = df_final_robbu[df_final_robbu['CORINGA1'].notnull() & df_final_robbu['CORINGA2'].isnull() & df_final_robbu['CORINGA3'].isnull()]
df_carga_robbu_2 = df_final_robbu[df_final_robbu['CORINGA1'].notnull() & df_final_robbu['CORINGA2'].notnull() & df_final_robbu['CORINGA3'].isnull()]
df_carga_robbu_3 = df_final_robbu[df_final_robbu['CORINGA1'].notnull() & df_final_robbu['CORINGA2'].notnull() & df_final_robbu['CORINGA3'].notnull()]


# Gerar saídas Robbu
caminho_robbu = 
df_carga_robbu_1.to_csv(f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_robbu_1.csv", sep=';', index=False, encoding='cp1252' )
df_carga_robbu_2.to_csv(f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_robbu_2.csv", sep=';', index=False, encoding='cp1252')
df_carga_robbu_3.to_csv(f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_robbu_3.csv", sep=';', index=False, encoding='cp1252')



# In[8]:


#Tirar possível duplicidade de linhas:

df_pipefy_todo = df_pipefy_todo.drop_duplicates()

df_pipefy_wpp = df_pipefy_wpp.drop_duplicates()

df_pipefy_dis = df_pipefy_dis.drop_duplicates()



# In[9]:


df_pipefy_todo.columns


# In[10]:


# Definir a formatação desejada para cada exportação de dataframe
formatos = {
    'Novo Preço Final Limite': '0.0000',
    'Novo Preço Final (R$/l)': '0.0000',
    'CNPJ': '0',
    'LBR Médio Mensal': '0.0000',
    'VEDI_COD_CLI': '0',
    'CNPJ_BASICO': '0',
    'VEDI_COD_MAT': '0',
    'VEDI_DAT_VEN': 'dd/mm/yyyy',
    'VEDI_COD_BAS': '0',
    'Distância': '0.0000',
}

# Definir os caminhos para salvar os arquivos
caminho_pipefy_dis = f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_pipefy_dis.xlsx"
caminho_pipefy_wpp = f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_pipefy_wpp.xlsx"
caminho_pipefy_todo = f"abfss://general@stedlk01distdigdev.dfs.core.windows.net/sandbox/engenharia/files/outputs/mailing/{data_atual}_carga_pipefy_todo.xlsx"


# Aplicar formatação desejada para os arquivos Excel 
with pd.ExcelWriter(caminho_pipefy_dis, engine='xlsxwriter') as writer_dis:
    df_pipefy_dis.to_excel(writer_dis, index=False)
    workbook_dis = writer_dis.book
    worksheet_dis = writer_dis.sheets['Sheet1']
    for col_name, formato in formatos.items():
        if formato == 'dd/mm/yyyy':
            date_format_dis = workbook_dis.add_format({'num_format': formato})
            worksheet_dis.set_column(df_pipefy_dis.columns.get_loc(col_name), df_pipefy_dis.columns.get_loc(col_name), None, date_format_dis)
        else:
            cell_format_dis = workbook_dis.add_format({'num_format': formato})
            worksheet_dis.set_column(df_pipefy_dis.columns.get_loc(col_name), df_pipefy_dis.columns.get_loc(col_name), None, cell_format_dis)

with pd.ExcelWriter(caminho_pipefy_wpp, engine='xlsxwriter') as writer_wpp:
    df_pipefy_wpp.to_excel(writer_wpp, index=False)
    workbook_wpp = writer_wpp.book
    worksheet_wpp = writer_wpp.sheets['Sheet1']
    for col_name, formato in formatos.items():
        if formato == 'dd/mm/yyyy':
            date_format_wpp = workbook_wpp.add_format({'num_format': formato})
            worksheet_wpp.set_column(df_pipefy_wpp.columns.get_loc(col_name), df_pipefy_wpp.columns.get_loc(col_name), None, date_format_wpp)
        else:
            cell_format_wpp = workbook_wpp.add_format({'num_format': formato})
            worksheet_wpp.set_column(df_pipefy_wpp.columns.get_loc(col_name), df_pipefy_wpp.columns.get_loc(col_name), None, cell_format_wpp)

with pd.ExcelWriter(caminho_pipefy_todo, engine='xlsxwriter') as writer_todo:
    df_pipefy_todo.to_excel(writer_todo, index=False)
    workbook_todo = writer_todo.book
    worksheet_todo = writer_todo.sheets['Sheet1']
    for col_name, formato in formatos.items():
        if formato == 'dd/mm/yyyy':
            date_format_todo = workbook_todo.add_format({'num_format': formato})
            worksheet_todo.set_column(df_pipefy_todo.columns.get_loc(col_name), df_pipefy_todo.columns.get_loc(col_name), None, date_format_todo)
        else:
            cell_format_todo = workbook_todo.add_format({'num_format': formato})
            worksheet_todo.set_column(df_pipefy_todo.columns.get_loc(col_name), df_pipefy_todo.columns.get_loc(col_name), None, cell_format_todo)
            


# In[11]:


# Verificar se as colunas "Flag" e "Score" existem no DataFrame
if "Flag" not in df_pipefy_todo.columns:
    # Criar a coluna "Flag" e preencher com ""
    df_pipefy_todo["Flag"] = ""
if "Score" not in df_pipefy_todo.columns:
    # Criar a coluna "Score" e preencher com ""
    df_pipefy_todo["Score"] = ""


# Renomear as colunas para inserção em tabela spark

df_pipefy_todo = df_pipefy_todo.rename(columns={
    "Nome Cliente": "nome_cliente",
    "Grupo Cliente": "grupo_cliente",
    "AECO": "aeco",
    "Segmento": "segmento",
    "LBR Médio Mensal": "lbr_medio_mensal",
    "Volume Médio Mensal": "volume_medio_mensal",
    "ZV_BR": "zv_br",
    "TITULAR": "titular",
    "H_N3": "h_n3",
    "H_N2": "h_n2",
    "Canal Fonte": "canal_fonte",
    "VEDI_COD_CLI": "vedi_cod_cli",
    "CLIE_NOM_MUNICIPIO": "clie_nom_municipio",
    "CLIE_NOM_ESTADO": "clie_nom_estado",
    "CNPJ_BASICO": "cnpj_basico",
    "CEP": "cep",
    "BAIRRO": "bairro",
    "RUA": "rua",
    "NUM": "num",
    "COMPLEMENTO": "complemento",
    "VEDI_COD_MAT": "vedi_cod_mat",
    "VEDI_DSC_MAT": "vedi_dsc_mat",
    "VEDI_DAT_VEN": "vedi_dat_ven",
    "VEDI_VOL": "vedi_vol",
    "VEDI_DSC_ICT": "vedi_dsc_ict",
    "Produto_D": "produto_d",
    "VEDI_COD_BAS": "vedi_cod_bas",
    "Base": "base",
    "Lista de Preço": "lista_de_preco",
    "Exposição Total": "exposicao_total",
    "% Exposição": "exposicao",
    "Limite de credito": "limite_de_credito",
    "Limite de Crédito": "limite_de_credito_2",
    "Distância": "distancia",
    "Market": "market",
    "SLA DE ENTREGA": "sla_de_entrega",
    "1": "1",
    "2": "2",
    "3": "3",
    "4": "4",
    "5": "5",
    "6": "6",
    "7": "7",
    "8": "8",
    "9": "9",
    "10": "10",
    "Novo Preço Final (R$/l)": "novo_preco_final_r_l",
    "Dias De Prazo": "dias_de_prazo",
    "GRUPO_CNPJ_BASICO": "grupo_cnpj_basico",
    "Novo Preço Final Limite": "novo_preco_final_limite",
    "CNPJ": "cnpj",
    "fone_1": "fone_1",
    "fone_2": "fone_2",
    "flag_mkt": "flag_mkt",
    "Fonte": "fonte",
    "Território": "territorio",
    "Grupo": "grupo",
    "Cluster": "cluster",
    "Terça Feira - 16/04": "terca_feira_16_04",
    "Quarta Feira - 17/04": "quarta_feira_17_04",
    "Quinta Feira - 18/04": "quinta_feira_18_04",
    "Sexta Feira - 19/04": "sexta_feira_19_04",
    "Segunda Feira - 22/04": "segunda_feira_22_04",
    "Canal": "canal",
    "Score": "score",
    "Flag": "flag"
})

# Criar um novo DataFrame contendo apenas as colunas renomeadas
df_pipefy_renomeado = df_pipefy_todo.loc[:, [
    "nome_cliente",
    "grupo_cliente",
    "aeco",
    "segmento",
    "lbr_medio_mensal",
    "volume_medio_mensal",
    "zv_br",
    "titular",
    "h_n3",
    "h_n2",
    "canal_fonte",
    "vedi_cod_cli",
    "clie_nom_municipio",
    "clie_nom_estado",
    "cnpj_basico",
    "cep",
    "bairro",
    "rua",
    "num",
    "complemento",
    "vedi_cod_mat",
    "vedi_dsc_mat",
    "vedi_dat_ven",
    "vedi_vol",
    "vedi_dsc_ict",
    "produto_d",
    "vedi_cod_bas",
    "base",
    "lista_de_preco",
    "exposicao_total",
    "exposicao",
    "limite_de_credito",
    "limite_de_credito_2",
    "distancia",
    "market",
    "sla_de_entrega",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "novo_preco_final_r_l",
    "dias_de_prazo",
    "grupo_cnpj_basico",
    "novo_preco_final_limite",
    "cnpj",
    "fone_1",
    "fone_2",
    "flag_mkt",
    "fonte",
    "territorio",
    "grupo",
    "cluster",
    "terca_feira_16_04",
    "quarta_feira_17_04",
    "quinta_feira_18_04",
    "sexta_feira_19_04",
    "segunda_feira_22_04",
    "canal",
    "score",
    "flag"
]]



# Definir o esquema da tabela Spark
schema = {
    "nome_cliente": str,
    "grupo_cliente": str,
    "aeco": str,
    "segmento": str,
    "lbr_medio_mensal": float,
    "volume_medio_mensal": float,
    "zv_br": str,
    "titular": str,
    "h_n3": str,
    "h_n2": str,
    "canal_fonte": str,
    "vedi_cod_cli": str,
    "clie_nom_municipio": str,
    "clie_nom_estado": str,
    "cnpj_basico": str,
    "cep": str,
    "bairro": str,
    "rua": str,
    "num": str,
    "complemento": str,
    "vedi_cod_mat": str,
    "vedi_dsc_mat": str,
    "vedi_dat_ven": str,
    "vedi_vol": float,
    "vedi_dsc_ict": str,
    "produto_d": str,
    "vedi_cod_bas": str,
    "base": str,
    "lista_de_preco": str,
    "exposicao_total": str,
    "exposicao": str,
    "limite_de_credito": str,
    "limite_de_credito_2": str,
    "distancia": float,
    "market": str,
    "sla_de_entrega": str,
    "1": str,
    "2": str,
    "3": str,
    "4": str,
    "5": str,
    "6": str,
    "7": str,
    "8": str,
    "9": str,
    "10": str,
    "novo_preco_final_r_l": float,
    "dias_de_prazo": str,
    "grupo_cnpj_basico": str,
    "novo_preco_final_limite": float,
    "cnpj": str,
    "fone_1": str,
    "fone_2": str,
    "flag_mkt": str,
    "fonte": str,
    "territorio": str,
    "grupo": str,
    "cluster": str,
    "terca_feira_16_04": str,
    "quarta_feira_17_04": str,
    "quinta_feira_18_04": str,
    "sexta_feira_19_04": str,
    "segunda_feira_22_04": str,
    "canal": str,
    "score": str,
    "flag": str
}

# Realizar o cast das colunas do DataFrame
df_pipefy_renomeado = df_pipefy_renomeado.astype(schema)
#df_pipefy_renomeado.insert(0, "id_oportunidade", 000)
df_pipefy_renomeado.columns


# In[12]:


import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

# Renomear coluna
df_codigos_sap = df_pipefy_renomeado.rename(columns={'vedi_cod_cli': 'cod_sap'})

# Converter para inteiro
df_codigos_sap['cod_sap'] = df_codigos_sap['cod_sap'].astype(int)

# Selecionar apenas a coluna 'cod_sap'
df_sap = df_codigos_sap[['cod_sap']]

# Criar DataFrame Spark com o esquema especificado
schema_cod = StructType([
    StructField("cod_sap", IntegerType(), False)
])
df_id_spark = spark.createDataFrame(df_sap, schema=schema_cod)


# **Abaixo há inserção de dados em tabela spark **

# In[13]:


from pyspark.sql.functions import col, lit, concat, date_format, format_string, row_number
from pyspark.sql.window import Window

# Obter a data específica:
data_atual = lit(data_atual)

# Criando alias para os DataFrames
df_id_spark_alias = df_id_spark.alias("df_id_spark")
df_wrk_id_oportunidade_alias = spark.read.table("workspace.tb_wrk_id_oportunidade").alias("wrk_id_oportunidade")

# Juntando os DataFrames e fazendo a seleção
df_verificar = df_id_spark_alias.join(
    df_wrk_id_oportunidade_alias.filter(col("dt_oportunidade") == data_atual),
    "cod_sap",
    "left"
).select(
    "cod_sap",
    col("wrk_id_oportunidade.id_oportunidade").isNull().alias("inserir_novo_registro")
).distinct()

# Define a janela de ordenação
window = Window.orderBy("cod_sap")

# Gera os novos IDs de oportunidade
df_novo_id_oportunidade = df_verificar.filter("inserir_novo_registro").withColumn(
    "id_oportunidade",
    concat(
        date_format(data_atual, "yyMMdd"),
        format_string("%06d", row_number().over(window))
    )
)

df_novo_id = df_novo_id_oportunidade.toPandas()

# Se houver novos registros para inserir, salve-os na tabela sandbox.tb_s_id_oportunidade
if df_novo_id_oportunidade.count() > 0:
    df_novo_id_oportunidade = df_novo_id_oportunidade.select(
        col("cod_sap"),
        col("id_oportunidade").alias("id_oportunidade"),  
        lit(data_atual.cast("date")).alias("dt_oportunidade")
    ).write.format("delta").mode("overwrite").partitionBy('dt_oportunidade').saveAsTable("workspace.tb_wrk_id_oportunidade")

# Verificando o esquema e mostrando uma amostra dos dados
# df_novo_id_oportunidade.printSchema()
# df_novo_id_oportunidade.show()


# In[15]:


# Converter DataFrame Pandas para DataFrame Spark
df_spark = spark.createDataFrame(df_pipefy_renomeado)

# Lendo a tabela workspace.tb_s_id_oportunidade e filtrando os dados para a data atual
df_id_oportunidade = spark.read.table("workspace.tb_wrk_id_oportunidade") \
    .filter(col("dt_oportunidade") == data_atual)

# Fazer o join entre os DataFrames
df_spark = df_spark.join(df_id_oportunidade.select("id_oportunidade", "cod_sap"),
                         col("vedi_cod_cli") == col("cod_sap"), 
                         "left_outer").drop("cod_sap")


fuso_horario_brasilia = "America/Sao_Paulo"

# Convertendo a coluna 'vedi_dat_ven' para o tipo DateType
df_spark = df_spark.withColumn('vedi_dat_ven', to_date(col('vedi_dat_ven'), 'yyyy-MM-dd'))
df_spark = df_spark.withColumn('dt_mailing', data_atual)
df_spark = df_spark.withColumn('dt_mailing', to_date(col('dt_mailing'), 'yyyy-MM-dd'))
# Adicionar colunas de timestamp da carga e data no formato "yyyy-mm-dd"
#df_spark = df_spark.withColumn("ts_carga", from_utc_timestamp(current_timestamp(), fuso_horario_brasilia))


# In[16]:


#Escrita na tabela
df_spark.write.format("delta")\
    .mode("overwrite")\
    .partitionBy('dt_mailing')\
    .saveAsTable("workspace.tb_wrk_mailing_canais")

