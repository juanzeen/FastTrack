import os
import socket
import hashlib
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from storage.postgres import insert_download, update_download_status

load_dotenv()

logger = logging.getLogger(__name__)

DOWNLOAD_FOLDER = Path(os.getenv('DOWNLOAD_FOLDER', './downloaded_files'))
BUFFER_SIZE     = 4096
SOCKET_TIMEOUT  = 10

def verify_checksum(filepath, expected_checksum):
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        resultado = sha256.hexdigest()
        return resultado == expected_checksum
    except OSError as e:
        logger.error(f"Erro ao verificar checksum de '{filepath}': {e}")
        return False

def download_file(peer_name, ip_address, port, checksum, filename):
    if not DOWNLOAD_FOLDER.exists():
        logger.warning(f"Pasta compartilhada '{DOWNLOAD_FOLDER}' não existe. Criando...")
        DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

    filepath = DOWNLOAD_FOLDER / filename

    download_id = insert_download(filename, checksum, peer_name, ip_address)
    if not download_id:
        logger.error("Não foi possível registrar o download no banco.")
        return 'failed'

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)

        try:
            sock.connect((ip_address, port))
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            logger.error(f"Peer '{peer_name}' ({ip_address}:{port}) indisponível: {e}")
            update_download_status(download_id, 'failed')
            return 'failed'

        requisicao = json.dumps({
            'type':     'DOWNLOAD_REQ',
            'checksum': checksum
        }) + '\n'

        sock.sendall(requisicao.encode('utf-8'))

        header_raw = _receive_line(sock)
        if not header_raw:
            logger.error(f"Peer '{peer_name}' não respondeu ao DOWNLOAD_REQ.")
            update_download_status(download_id, 'failed')
            return 'failed'

        try:
            header = json.loads(header_raw)
        except json.JSONDecodeError:
            logger.error(f"Header inválido recebido de '{peer_name}': {header_raw}")
            update_download_status(download_id, 'failed')
            return 'failed'

        if header.get('type') == 'ERROR':
            logger.warning(f"Peer '{peer_name}' recusou download: {header.get('reason')}")
            update_download_status(download_id, 'canceled')
            return 'canceled'

        if header.get('type') != 'FILE_DATA':
            logger.error(f"Resposta inesperada de '{peer_name}': {header}")
            update_download_status(download_id, 'failed')
            return 'failed'

        total_bytes = int(header.get('size_bytes', 0))

        bytes_recebidos = 0

        with open(filepath, 'wb') as f:
            while bytes_recebidos < total_bytes:
                restante = total_bytes - bytes_recebidos
                chunk = sock.recv(min(BUFFER_SIZE, restante))

                if not chunk:
                    logger.error(
                        f"Conexão encerrada prematuramente com '{peer_name}'. "
                        f"Recebidos {bytes_recebidos}/{total_bytes} bytes."
                    )
                    break

                f.write(chunk)
                bytes_recebidos += len(chunk)

        if bytes_recebidos < total_bytes:
            logger.error(f"Download incompleto de '{filename}': {bytes_recebidos}/{total_bytes} bytes.")
            _delete_incomplete_file(filepath)
            update_download_status(download_id, 'failed')
            return 'failed'

        if not verify_checksum(str(filepath), checksum):
            logger.error(f"Checksum inválido para '{filename}'. Arquivo corrompido, descartando.")
            _delete_incomplete_file(filepath)
            update_download_status(download_id, 'failed')
            return 'failed'

        logger.info(f"Download de '{filename}' concluído com sucesso ({bytes_recebidos} bytes).")
        update_download_status(download_id, 'completed')
        return 'completed'

    except socket.timeout:
        logger.error(f"Timeout na conexão com peer '{peer_name}' ({ip_address}:{port}).")
        _delete_incomplete_file(filepath)
        update_download_status(download_id, 'failed')
        return 'failed'

    except Exception as e:
        logger.error(f"Erro inesperado no download de '{filename}': {e}")
        _delete_incomplete_file(filepath)
        update_download_status(download_id, 'failed')
        return 'failed'

    finally:
        if sock:
            sock.close()

def _receive_line(sock):
    linha = b''
    try:
        while True:
            char = sock.recv(1)
            if not char:
                return None
            if char == b'\n':
                return linha.decode('utf-8').strip()
            linha += char
    except Exception:
        return None


def _delete_incomplete_file(filepath):
    try:
        if filepath.exists():
            filepath.unlink()
            logger.info(f"Arquivo incompleto '{filepath.name}' removido.")
    except OSError as e:
        logger.error(f"Erro ao remover arquivo incompleto '{filepath}': {e}")