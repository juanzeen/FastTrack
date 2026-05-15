import socket
import logging
import hashlib
import os
from pathlib import Path

from network import protocol as proto
from exceptions import DownloadError
import config

logger = logging.getLogger(__name__)


def _connect(ip: str, port: int):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.SOCKET_TIMEOUT)
        sock.connect((ip, port))
        return sock
    except socket.timeout:
        logger.warning(f"Timeout ao conectar com {ip}:{port}.")
        return None
    except ConnectionRefusedError:
        logger.warning(f"Conexão recusada por {ip}:{port}.")
        return None
    except OSError as e:
        logger.warning(f"Erro de rede ao conectar com {ip}:{port}: {e}")
        return None


def send_and_receive(ip: str, port: int, message: bytes) -> dict | None:
    sock = _connect(ip, port)
    if not sock:
        return None
    try:
        sock.sendall(message)
        return proto.receive_message(sock)
    except (BrokenPipeError, ConnectionResetError) as e:
        logger.error(f"Conexão perdida com {ip}:{port}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado na comunicação com {ip}:{port}: {e}")
        return None
    finally:
        sock.close()


# ── descoberta ────────────────────────────────────────────────

def who_is_leader(ip: str, port: int) -> dict | None:
    try:
        return send_and_receive(ip, port, proto.build(proto.WHO_IS_LEADER))
    except Exception as e:
        logger.error(f"Erro ao perguntar quem é o líder para {ip}:{port}: {e}")
        return None


def announce(leader_ip: str, leader_port: int,
             peer_name: str, ip: str, port: int,
             uptime: float, files: list) -> dict | None:
    try:
        return send_and_receive(
            leader_ip, leader_port,
            proto.build(
                proto.ANNOUNCE,
                peer_name=peer_name,
                ip_address=ip,
                port=port,
                uptime=uptime,
                files=files
            )
        )
    except Exception as e:
        logger.error(f"Erro ao anunciar para o líder {leader_ip}:{leader_port}: {e}")
        return None


def request_file_index(ip: str, port: int) -> list:
    try:
        resp = send_and_receive(ip, port, proto.build(proto.FILE_INDEX))
        if resp and resp.get('type') == proto.FILE_INDEX_RESP:
            return resp.get('files', [])
    except Exception as e:
        logger.error(f"Erro ao solicitar índice de arquivos para {ip}:{port}: {e}")
    return []


# ── heartbeat ─────────────────────────────────────────────────

def heartbeat(ip: str, port: int) -> bool:
    try:
        resp = send_and_receive(ip, port, proto.build(proto.HEARTBEAT))
        return resp is not None and resp.get('type') == proto.HEARTBEAT_ACK
    except Exception:
        return False


# ── eleição ───────────────────────────────────────────────────

def send_election(ip: str, port: int, peer_name: str, uptime: float) -> bool:
    try:
        resp = send_and_receive(
            ip, port,
            proto.build(proto.ELECTION, peer_name=peer_name, uptime=uptime)
        )
        return resp is not None and resp.get('type') == proto.ELECTION_OK
    except Exception as e:
        logger.error(f"Erro ao enviar eleição para {ip}:{port}: {e}")
        return False


def broadcast_leader(peers: list, peer_name: str, ip: str, port: int):
    try:
        msg = proto.build(proto.LEADER, peer_name=peer_name,
                          ip_address=ip, port=port)
        for peer in peers:
            send_and_receive(peer['ip_address'], peer['port'], msg)
    except Exception as e:
        logger.error(f"Erro ao propagar novo líder: {e}")


# ── download ──────────────────────────────────────────────────

def download_file(peer_name: str, ip: str, port: int,
                  checksum: str, expected_filename: str) -> str:
    download_folder = Path(config.DOWNLOAD_FOLDER)
    try:
        download_folder.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Erro ao criar pasta de download '{download_folder}': {e}")
        return 'failed'

    sock = _connect(ip, port)
    if not sock:
        return 'failed'

    filepath = download_folder / expected_filename
    try:
        sock.sendall(proto.build(proto.DOWNLOAD_REQ, checksum=checksum))

        header = proto.receive_message(sock)
        if not header:
            logger.error(f"Sem resposta ao DOWNLOAD_REQ de '{peer_name}'.")
            return 'failed'

        if header.get('type') == proto.ERROR:
            reason = header.get('reason')
            logger.warning(f"Peer '{peer_name}' recusou download: {reason}")
            return 'not_found' if reason == 'file_not_found' else 'failed'

        if header.get('type') != proto.FILE_DATA:
            logger.error(f"Resposta inesperada do peer '{peer_name}': {header.get('type')}")
            return 'failed'

        filename   = header.get('filename', expected_filename)
        total_size = int(header.get('size_bytes', 0))
        filepath   = download_folder / filename

        sha256          = hashlib.sha256()
        bytes_recebidos = 0

        with open(filepath, 'wb') as f:
            while bytes_recebidos < total_size:
                restante = total_size - bytes_recebidos
                chunk    = sock.recv(min(4096, restante))
                if not chunk:
                    break
                f.write(chunk)
                sha256.update(chunk)
                bytes_recebidos += len(chunk)

        if bytes_recebidos < total_size:
            raise DownloadError(f"Download incompleto: {bytes_recebidos}/{total_size} bytes.")

        if sha256.hexdigest() != checksum:
            raise DownloadError(f"Checksum inválido para '{filename}'. Arquivo corrompido.")

        logger.info(f"Download de '{filename}' concluído ({bytes_recebidos} bytes).")
        return 'completed'

    except (socket.timeout, ConnectionResetError, BrokenPipeError) as e:
        logger.error(f"Erro de rede durante o download de '{peer_name}': {e}")
        _delete_file(filepath)
        return 'failed'
    except DownloadError as e:
        logger.error(e)
        _delete_file(filepath)
        return 'failed'
    except Exception as e:
        logger.error(f"Erro inesperado no download: {e}")
        _delete_file(filepath)
        return 'failed'
    finally:
        sock.close()


def _delete_file(filepath: Path):
    try:
        if filepath.exists():
            filepath.unlink()
    except OSError as e:
        logger.error(f"Erro ao remover arquivo temporário '{filepath}': {e}")
