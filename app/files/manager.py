import os
import hashlib
import logging
from pathlib import Path
from exceptions import FileOperationError
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
    except (OSError, IOError) as e:
        logger.error(f"Erro ao calcular checksum de '{filepath}': {e}")
        return None


def scan_shared_folder() -> list[dict]:
    try:
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
                'filepath':   str(entry.resolve()),
                'size_bytes': entry.stat().st_size,
                'checksum':   checksum
            })

        logger.info(f"Scan: {len(arquivos)} arquivo(s) em '{SHARED_FOLDER}'.")
        return arquivos
    except Exception as e:
        logger.error(f"Erro ao escanear pasta compartilhada '{SHARED_FOLDER}': {e}")
        return []


def get_file_for_download(checksum: str, peer_name: str) -> dict | None:
    try:
        arquivos = scan_shared_folder()
        arquivo  = next((f for f in arquivos if f['checksum'] == checksum), None)

        if not arquivo:
            logger.warning(f"Arquivo com checksum '{checksum}' não encontrado.")
            return None

        if not os.path.exists(arquivo['filepath']):
            logger.warning(f"Arquivo '{arquivo['filename']}' sumiu do disco.")
            return None

        return arquivo
    except Exception as e:
        logger.error(f"Erro ao localizar arquivo para download (checksum: {checksum}): {e}")
        return None
