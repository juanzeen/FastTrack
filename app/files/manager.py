import os
import hashlib
import logging
from pathlib import Path
import config

logger = logging.getLogger(__name__)

SHARED_FOLDER = Path(config.SHARED_FOLDER)


def calculate_checksum(filepath: str) -> str | None:
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    except OSError as e:
        logger.error(f"Erro ao calcular checksum de '{filepath}': {e}")
        return None


def scan_shared_folder() -> list[dict]:
    """
    Varre a pasta compartilhada e retorna os arquivos encontrados.
    Nunca inclui filepath — dado interno, não sai da máquina.
    """
    if not SHARED_FOLDER.exists():
        SHARED_FOLDER.mkdir(parents=True, exist_ok=True)
        return []

    arquivos = []
    for entry in SHARED_FOLDER.iterdir():
        if not entry.is_file():
            continue

        checksum = calculate_checksum(str(entry))
        if not checksum:
            continue

        arquivos.append({
            'filename':   entry.name,
            'filepath':   str(entry.resolve()),  # filepath só existe localmente
            'size_bytes': entry.stat().st_size,
            'checksum':   checksum
        })

    logger.info(f"Scan: {len(arquivos)} arquivo(s) em '{SHARED_FOLDER}'.")
    return arquivos


def get_file_for_download(checksum: str, peer_name: str) -> dict | None:
    """
    Localiza o arquivo no disco pelo checksum.
    Chamado pelo servidor quando outro peer solicita download.
    Retorna None se não encontrado ou se sumiu do disco.
    """
    arquivos = scan_shared_folder()
    arquivo  = next((f for f in arquivos if f['checksum'] == checksum), None)

    if not arquivo:
        logger.warning(f"Arquivo com checksum '{checksum}' não encontrado.")
        return None

    if not os.path.exists(arquivo['filepath']):
        logger.warning(f"Arquivo '{arquivo['filename']}' sumiu do disco.")
        return None

    return arquivo
