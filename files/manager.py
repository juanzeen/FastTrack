import os
import hashlib
import logging
from pathlib import Path
from dotenv import load_dotenv
from storage.postgres import insert_shared_file, get_shared_files_by_peer, delete_shared_file, get_shared_files_by_checksum


load_dotenv()

logger = logging.getLogger(__name__)

SHARED_FOLDER = Path(os.getenv('SHARED_FOLDER', './shared_files'))

def calculate_checksum(filepath):
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    except OSError as e:
        logger.error(f"Erro ao calcular checksum de '{filepath}': {e}")
        return None

def scan_shared_folder():
    if not SHARED_FOLDER.exists():
        logger.warning(f"Pasta compartilhada '{SHARED_FOLDER}' não existe. Criando...")
        SHARED_FOLDER.mkdir(parents=True, exist_ok=True)
        return []

    arquivos = []

    for entry in SHARED_FOLDER.iterdir():
        if not entry.is_file():
            continue

        checksum = calculate_checksum(str(entry))
        if checksum is None:
            logger.warning(f"Arquivo '{entry.name}' ignorado — não foi possível calcular checksum.")
            continue

        arquivos.append({
            'filename':   entry.name,
            'filepath':   str(entry.resolve()),
            'size_bytes': entry.stat().st_size,
            'checksum':   checksum
        })

    logger.info(f"Scan concluído: {len(arquivos)} arquivo(s) encontrado(s) em '{SHARED_FOLDER}'.")
    return arquivos

def sync_shared_files(peer_name):
    arquivos_disco = scan_shared_folder()
    arquivos_banco = get_shared_files_by_peer(peer_name)

    checksums_disco = {f['checksum'] for f in arquivos_disco}
    checksums_banco = {f['checksum'] for f in arquivos_banco}

    novos = [f for f in arquivos_disco if f['checksum'] not in checksums_banco]

    removidos = checksums_banco - checksums_disco

    inserted = 0
    for arquivo in novos:
        ok = insert_shared_file(
            peer_name,
            arquivo['filename'],
            arquivo['filepath'],
            arquivo['size_bytes'],
            arquivo['checksum']
        )
        if ok:
            inserted += 1

    deleted = 0
    for checksum in removidos:
        ok = delete_shared_file(checksum, peer_name)
        if ok:
            deleted += 1

    total = len(checksums_disco)

    logger.info(
        f"Sync concluído para '{peer_name}': "
        f"+{inserted} inserido(s), -{deleted} removido(s), "
        f"{total} total."
    )

    return {
        'inserted': inserted,
        'deleted':  deleted,
        'total':    total
    }

def get_file_for_download(checksum, peer_name):
    resultados = get_shared_files_by_checksum(checksum)

    if not resultados:
        logger.warning(f"Checksum '{checksum}' não encontrado no banco.")
        return None

    arquivo = next((r for r in resultados if r['peer_name'] == peer_name), None)

    if not arquivo:
        logger.warning(f"Peer '{peer_name}' não possui o arquivo com checksum '{checksum}'.")
        return None

    if not os.path.exists(arquivo['filepath']):
        logger.warning(
            f"Arquivo '{arquivo['filename']}' sumiu do disco. "
            f"Removendo do banco."
        )
        delete_shared_file(checksum, peer_name)
        return None

    return {
        'filename':   arquivo['filename'],
        'filepath':   arquivo['filepath'],
        'size_bytes': arquivo['size_bytes'],
        'checksum':   arquivo['checksum']
    }

def get_files_to_announce(peer_name):
    arquivos = get_shared_files_by_peer(peer_name)

    return [
        {
            'filename':   f['filename'],
            'size_bytes': f['size_bytes'],
            'checksum':   f['checksum']
        }
        for f in arquivos
    ]