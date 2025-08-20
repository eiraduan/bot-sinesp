import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

# --- Configurações do Banco de Dados ---
# Substitua as informações de conexão abaixo com as suas credenciais

# --- Configurações do Banco de Dados ---
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT", 5432) # O segundo argumento é o valor padrão, caso não encontre

# --- Configurações do Processamento ---
pasta_arquivos = "arquivos"
estado_filtro = "RO"
tabela_destino = "dados_sinesp"

# ---

print("Iniciando o processo de ETL (Extrair, Transformar, Carregar)...")


# 1. Configura a conexão com o PostgreSQL
try:
    url_object = URL.create(
        "postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
    )
    engine = create_engine(url_object)
    print("Conexão com o banco de dados estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

# 2. Percorre os arquivos na pasta e processa cada um
arquivos = [f for f in os.listdir(pasta_arquivos) if f.endswith('.xlsx')]

if not arquivos:
    print(f"Nenhum arquivo .xlsx encontrado na pasta '{pasta_arquivos}'.")
else:
    for arquivo in arquivos:
        caminho_completo = os.path.join(pasta_arquivos, arquivo)
        
        try:
            print(f"\nProcessando o arquivo: {arquivo}")
            
            # Lê o arquivo Excel completo para um DataFrame do pandas
            df = pd.read_excel(caminho_completo)
            
            # Filtra os dados apenas para o estado de Rondônia
            # A coluna de estado pode ser 'uf' ou 'UF', então verificamos ambas.
            coluna_uf = 'uf' if 'uf' in df.columns.str.lower() else 'UF'
            df_ro = df[df[coluna_uf].str.upper() == estado_filtro].copy()
            
            # Converte a coluna 'data_referencia' para o formato de data (datetime)
            # O formato de entrada é dd-mm-yyyy, então passamos isso no parâmetro 'format'

            df_ro['data_referencia'] = pd.to_datetime(df_ro['data_referencia'], format='%d-%m-%Y', errors='coerce')

            # Cria as novas colunas 'ano' e 'mes' a partir da coluna 'data_referencia'
            df_ro['ano'] = df_ro['data_referencia'].dt.year
            df_ro['mes'] = df_ro['data_referencia'].dt.month
            
            if not df_ro.empty:
                print(f"  {len(df_ro)} linhas encontradas para {estado_filtro}.")
                
                # 3. Salva os dados no banco de dados
                df_ro.to_sql(
                    name=tabela_destino,
                    con=engine,
                    if_exists='append', # Adiciona os dados à tabela existente
                    index=False # Não salva o índice do DataFrame como uma coluna
                )
                print(f"  Dados de {arquivo} salvos na tabela '{tabela_destino}' com sucesso.")
            else:
                print(f"  Nenhuma linha encontrada para {estado_filtro} no arquivo {arquivo}.")
                
        except Exception as e:
            print(f"Erro ao processar o arquivo {arquivo}: {e}")

print("\nProcessamento finalizado.")