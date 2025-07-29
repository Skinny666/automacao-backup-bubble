import os
import requests
import time
import csv
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Carregar variáveis de ambiente dos arquivos .env
load_dotenv("bubble.env")
load_dotenv("google.env")

# Variáveis de ambiente
PASTA_DRIVE_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
CREDENCIAIS_GOOGLE_DRIVE = os.getenv("GOOGLE_CREDENTIALS_PATH")
API_TOKEN = os.getenv("BUBBLE_API_TOKEN")

# Cabeçalhos da requisição
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def autenticar_google_drive():
    creds = Credentials.from_service_account_file(
        CREDENCIAIS_GOOGLE_DRIVE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def criar_subpasta(pasta_drive_id, nome_subpasta):
    service = autenticar_google_drive()
    file_metadata = {
        "name": nome_subpasta,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [pasta_drive_id]
    }
    subpasta = service.files().create(body=file_metadata, fields="id").execute()
    print(f"Subpasta '{nome_subpasta}' criada com sucesso! ID: {subpasta['id']}")
    return subpasta["id"]

def upload_para_google_drive(arquivo_local, pasta_drive_id):
    service = autenticar_google_drive()
    file_metadata = {
        "name": os.path.basename(arquivo_local),
        "parents": [pasta_drive_id]
    }
    media = MediaFileUpload(arquivo_local, resumable=True)
    arquivo = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"Arquivo '{arquivo_local}' enviado com sucesso! ID: {arquivo['id']}")

def salvar_dados_por_url(url_base, nome_arquivo, subpasta_drive_id):
    cursor = 0
    dados_completos = []

    while True:
        url = f"{url_base}?cursor={cursor}"
        print(f"\nBuscando dados com cursor={cursor}...")

        try:
            response = requests.get(url, headers=HEADERS)
            print(f"Status Code: {response.status_code}")

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"Limite de requisições atingido. Aguardando {retry_after} segundos...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            dados = response.json()

            if "response" not in dados or "results" not in dados["response"]:
                print(f"Estrutura inesperada na resposta da API. Encerrando.")
                break

            resultados = dados["response"]["results"]
            count = dados["response"].get("count", 0)

            if not resultados or count == 0:
                print(f"Nenhum dado restante. Finalizando busca para {nome_arquivo}.")
                break

            dados_completos.extend(resultados)
            print(f"{len(resultados)} registros adicionados (Total: {len(dados_completos)})")

            cursor += 100
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição: {e}")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")
            break

    if dados_completos:
        fieldnames = set()
        for linha in dados_completos:
            fieldnames.update(linha.keys())
        fieldnames = sorted(fieldnames)

        try:
            with open(nome_arquivo, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter="\t")
                writer.writeheader()
                writer.writerows(dados_completos)

            upload_para_google_drive(nome_arquivo, subpasta_drive_id)
            os.remove(nome_arquivo)
            print(f"Arquivo '{nome_arquivo}' enviado e removido localmente.")
        except Exception as e:
            print(f"Erro ao salvar ou enviar o arquivo '{nome_arquivo}': {e}")
    else:
        print(f"Nenhum dado coletado de {url_base}.")

if __name__ == "__main__":
    data_atual = datetime.now().strftime("%Y-%m-%d")
    subpasta_drive_id = criar_subpasta(PASTA_DRIVE_ID, data_atual)

    # Dicionário com as URLs das APIs e o nome desejado para o arquivo .tsv
    urls = {
        "https://api.seusistema.com.br/api/obj/tabela1": "tabela1.tsv",
        "https://api.seusistema.com.br/api/obj/tabela2": "tabela2.tsv",
        "https://api.seusistema.com.br/api/obj/tabela3": "tabela3.tsv",
        # Adicione mais endpoints conforme necessário
    }

    for url, arquivo in urls.items():
        print(f"\nIniciando coleta na URL: {url}")
        salvar_dados_por_url(url, arquivo, subpasta_drive_id)
