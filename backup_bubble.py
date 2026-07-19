import os
import sys
import csv
import time
import logging
from datetime import datetime

import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

load_dotenv("bubble.env")
load_dotenv("google.env")

PASTA_DRIVE_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
CREDENCIAIS_GOOGLE_DRIVE = os.getenv("GOOGLE_CREDENTIALS_PATH")
API_TOKEN = os.getenv("BUBBLE_API_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

REQUEST_TIMEOUT = 30  # segundos
MAX_RETRIES = 5
PAGE_LIMIT = 100  # quantos registros pedir por página (ajustável)

# Tabelas a exportar: adicione/remova aqui sem tocar no resto do código
TABELAS = [
    {"url": "https://api.seusistema.com.br/api/obj/tabela1", "arquivo": "tabela1.tsv"},
    {"url": "https://api.seusistema.com.br/api/obj/tabela2", "arquivo": "tabela2.tsv"},
    {"url": "https://api.seusistema.com.br/api/obj/tabela3", "arquivo": "tabela3.tsv"},
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("backup_bubble.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validação inicial
# ---------------------------------------------------------------------------

def validar_configuracao():
    faltando = [
        nome for nome, valor in {
            "GOOGLE_DRIVE_FOLDER_ID": PASTA_DRIVE_ID,
            "GOOGLE_CREDENTIALS_PATH": CREDENCIAIS_GOOGLE_DRIVE,
            "BUBBLE_API_TOKEN": API_TOKEN,
        }.items() if not valor
    ]
    if faltando:
        log.error(f"Variáveis de ambiente ausentes: {', '.join(faltando)}")
        sys.exit(1)
    if not os.path.exists(CREDENCIAIS_GOOGLE_DRIVE):
        log.error(f"Arquivo de credenciais não encontrado: {CREDENCIAIS_GOOGLE_DRIVE}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Google Drive (autenticação única, reaproveitada em todo o script)
# ---------------------------------------------------------------------------

_drive_service = None


def get_drive_service():
    global _drive_service
    if _drive_service is None:
        creds = Credentials.from_service_account_file(
            CREDENCIAIS_GOOGLE_DRIVE,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        _drive_service = build("drive", "v3", credentials=creds)
    return _drive_service


def encontrar_ou_criar_subpasta(pasta_drive_id, nome_subpasta):
    """Reaproveita a pasta do dia se ela já existir, em vez de duplicar."""
    service = get_drive_service()
    query = (
        f"name = '{nome_subpasta}' and "
        f"'{pasta_drive_id}' in parents and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    resultado = service.files().list(q=query, fields="files(id, name)").execute()
    encontrados = resultado.get("files", [])

    if encontrados:
        pasta_id = encontrados[0]["id"]
        log.info(f"Subpasta '{nome_subpasta}' já existe, reutilizando. ID: {pasta_id}")
        return pasta_id

    file_metadata = {
        "name": nome_subpasta,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [pasta_drive_id],
    }
    subpasta = service.files().create(body=file_metadata, fields="id").execute()
    log.info(f"Subpasta '{nome_subpasta}' criada. ID: {subpasta['id']}")
    return subpasta["id"]


def upload_para_google_drive(arquivo_local, pasta_drive_id) -> bool:
    """Retorna True se o upload foi confirmado (arquivo recebeu um ID)."""
    service = get_drive_service()
    file_metadata = {"name": os.path.basename(arquivo_local), "parents": [pasta_drive_id]}
    media = MediaFileUpload(arquivo_local, resumable=True)
    try:
        arquivo = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        if arquivo.get("id"):
            log.info(f"Upload OK: '{arquivo_local}' -> ID {arquivo['id']}")
            return True
        log.error(f"Upload de '{arquivo_local}' não retornou ID de confirmação.")
        return False
    except Exception as e:
        log.error(f"Falha no upload de '{arquivo_local}': {e}")
        return False


# ---------------------------------------------------------------------------
# Coleta de dados no Bubble
# ---------------------------------------------------------------------------

def buscar_pagina(session, url, cursor, tentativa=1):
    """Busca uma página com retry/backoff para erros transitórios."""
    try:
        resp = session.get(
            url, headers=HEADERS, params={"cursor": cursor, "limit": PAGE_LIMIT},
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            log.warning(f"Rate limit atingido. Aguardando {retry_after}s...")
            time.sleep(retry_after)
            return buscar_pagina(session, url, cursor, tentativa)

        resp.raise_for_status()
        return resp.json()

    except requests.exceptions.RequestException as e:
        if tentativa >= MAX_RETRIES:
            log.error(f"Falha após {MAX_RETRIES} tentativas em {url} (cursor={cursor}): {e}")
            return None
        espera = 2 ** tentativa  # backoff exponencial: 2, 4, 8, 16...
        log.warning(f"Erro de requisição ({e}). Tentativa {tentativa}/{MAX_RETRIES}, aguardando {espera}s...")
        time.sleep(espera)
        return buscar_pagina(session, url, cursor, tentativa + 1)


def coletar_tabela(session, url_base) -> list:
    cursor = 0
    dados_completos = []

    while True:
        log.info(f"Buscando '{url_base}' (cursor={cursor})...")
        dados = buscar_pagina(session, url_base, cursor)

        if dados is None:
            log.error(f"Interrompendo coleta de {url_base} por falha irrecuperável.")
            break

        if "response" not in dados or "results" not in dados["response"]:
            log.error(f"Estrutura inesperada na resposta de {url_base}. Encerrando.")
            break

        resultados = dados["response"]["results"]
        remaining = dados["response"].get("remaining", 0)

        if not resultados:
            log.info(f"Nenhum dado restante para {url_base}.")
            break

        dados_completos.extend(resultados)
        cursor += len(resultados)  # avança pelo tamanho real da página, não um valor fixo
        log.info(f"+{len(resultados)} registros (total: {len(dados_completos)}, restantes: {remaining})")

        if remaining <= 0:
            break

        time.sleep(0.5)  # gentileza com a API

    return dados_completos


def salvar_e_enviar(url_base, nome_arquivo, subpasta_drive_id, session) -> dict:
    """Retorna um resumo {arquivo, registros, status}."""
    dados_completos = coletar_tabela(session, url_base)

    if not dados_completos:
        log.warning(f"Nenhum dado coletado de {url_base}.")
        return {"arquivo": nome_arquivo, "registros": 0, "status": "sem dados"}

    fieldnames = sorted({chave for linha in dados_completos for chave in linha.keys()})

    try:
        with open(nome_arquivo, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(dados_completos)
    except Exception as e:
        log.error(f"Erro ao gravar '{nome_arquivo}': {e}")
        return {"arquivo": nome_arquivo, "registros": len(dados_completos), "status": "erro ao gravar"}

    sucesso = upload_para_google_drive(nome_arquivo, subpasta_drive_id)

    if sucesso:
        os.remove(nome_arquivo)  # só apaga localmente se o upload foi confirmado
        return {"arquivo": nome_arquivo, "registros": len(dados_completos), "status": "ok"}
    else:
        log.warning(f"Mantendo '{nome_arquivo}' localmente pois o upload falhou.")
        return {"arquivo": nome_arquivo, "registros": len(dados_completos), "status": "falha no upload"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    validar_configuracao()

    data_atual = datetime.now().strftime("%Y-%m-%d")
    subpasta_drive_id = encontrar_ou_criar_subpasta(PASTA_DRIVE_ID, data_atual)

    resumo = []
    with requests.Session() as session:
        for tabela in TABELAS:
            log.info(f"=== Iniciando: {tabela['url']} ===")
            resultado = salvar_e_enviar(tabela["url"], tabela["arquivo"], subpasta_drive_id, session)
            resumo.append(resultado)

    log.info("========== RESUMO DO BACKUP ==========")
    for item in resumo:
        log.info(f"{item['arquivo']}: {item['registros']} registros - {item['status'].upper()}")

    falhas = [r for r in resumo if r["status"] not in ("ok", "sem dados")]
    if falhas:
        log.error(f"{len(falhas)} tabela(s) com problema. Verifique o log acima.")
        sys.exit(1)

    log.info("Backup concluído com sucesso.")


if __name__ == "__main__":
    main()
