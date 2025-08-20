import clean_database
import create_table
import download
import insert


def run_pipeline():
    """
    Executa a sequência de scripts ETL.
    """
    print("Iniciando o pipeline de ETL...")

    print("1. Limpando o banco de dados...")
    clean_database  # Supondo que a função principal do seu script clean_database.py se chame 'run'

    print("2. Criando as tabelas...")
    #create_table.run() # Supondo que a função principal do seu script create_table.py se chame 'run'

    print("3. Baixando os dados...")
    download # Supondo que a função principal do seu script download.py se chame 'run'

    print("4. Inserindo os dados...")
    insert # Supondo que a função principal do seu script insert.py se chame 'run'

    print("Pipeline de ETL concluído com sucesso!")

if __name__ == "__main__":
    run_pipeline()