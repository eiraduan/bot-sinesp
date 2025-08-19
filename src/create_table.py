import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# --- Carrega as variáveis de ambiente do arquivo .env ---
load_dotenv()

# --- Configurações do Banco de Dados usando variáveis de ambiente ---
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

try:
    print("Tentando estabelecer a conexão com o banco de dados...")
    conexao = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    print("Conexão estabelecida com sucesso!")
    
    cursor = conexao.cursor()

    # --- Criação da Tabela (com verificação para evitar erros) ---
    print("Verificando e criando a tabela 'dados_sinesp' se ela não existir...")
    
    # Adiciona a verificação "IF NOT EXISTS" para evitar erros se a tabela já existir
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dados_sinesp (
        id SERIAL PRIMARY KEY,
        uf VARCHAR(2),
        municipio VARCHAR(255),
        evento VARCHAR(255),
        data_referencia DATE,
        agente VARCHAR(255),
        arma VARCHAR(255),
        faixa_etaria VARCHAR(255),
        feminino INTEGER,
        masculino INTEGER,
        nao_informado INTEGER,
        total_vitima INTEGER,
        total INTEGER,
        total_peso NUMERIC(10, 3),
        abrangencia VARCHAR(255),
        formulario VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    
    conexao.commit()
    print("Tabela 'dados_sinesp' verificada/criada com sucesso.")

except Exception as e:
    print(f"Erro: {e}")

finally:
    if 'conexao' in locals() and conexao:
        cursor.close()
        conexao.close()
        print("Conexão com o banco de dados fechada.")