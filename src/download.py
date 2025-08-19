import requests
from datetime import datetime
import os

# Define o ano inicial e o ano atual
ano_inicial = 2015
ano_atual = datetime.now().year

# Define o nome da pasta onde os arquivos serão salvos
pasta_destino = "arquivos"

# ---

# Verifica se a pasta já existe. Se não existir, ela será criada.
if not os.path.exists(pasta_destino):
    os.makedirs(pasta_destino)
    print(f"Pasta '{pasta_destino}' criada com sucesso.")

print("Iniciando o processo de download de arquivos anuais...")

# Loop para iterar por cada ano, do inicial até o atual
for ano in range(ano_inicial, ano_atual + 1):
    url = f"https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/download/dnsp-base-de-dados/bancovde-{ano}.xlsx/@@download/file"
    
    # Cria o caminho completo do arquivo, incluindo a pasta de destino
    caminho_completo = os.path.join(pasta_destino, f"BancoVDE_{ano}.xlsx")

    try:
        print(f"\nBaixando o arquivo do ano {ano}...")
        
        # Faz a requisição HTTP
        response = requests.get(url)
        response.raise_for_status() # Lança um erro para status de resposta ruins
        
        # Salva o arquivo no caminho completo
        with open(caminho_completo, 'wb') as f:
            f.write(response.content)
            
        print(f"Download de BancoVDE_{ano}.xlsx concluído com sucesso!")
        
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o arquivo de {ano}: {e}")
    except IOError as e:
        print(f"Erro ao salvar o arquivo de {ano}: {e}")

print("\nProcesso de download finalizado.")