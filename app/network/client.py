import socket
import logging
import hashlib
import os
from pathlib import Path

from network import protocol as proto
import config

logger = logging.getLogger(__name__)


def _connect(ip: str, port: int):
    """Abre conexão TCP com timeout. Retorna socket ou None."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.SOCKET_TIMEOUT)
        sock.connect((ip, port))
        return sock
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.warning(f"Peer {ip}:{port} indisponível: {e}")
        return None


def send_and_receive(ip: str, port: int, message: bytes) -> dict | None:
    """Envia mensagem e recebe resposta JSON. Fecha conexão ao fim."""
    sock = _connect(ip, port)
    if not sock:
        return None
    try:
        sock.sendall(message)
        return proto.receive_message(sock)
    except Exception as e:
        logger.error(f"Erro na comunicação com {ip}:{port}: {e}")
        return None
    finally:
        sock.close()


# ── descoberta ────────────────────────────────────────────────

def who_is_leader(ip: str, port: int) -> dict | None:
    """Pergunta para um peer quem é o líder atual."""
    return send_and_receive(ip, port, proto.build(proto.WHO_IS_LEADER))


def announce(leader_ip: str, leader_port: int,
             peer_name: str, ip: str, port: int,
             uptime: float, files: list) -> dict | None:
    """
    Anuncia presença ao super nó.
    Retorna PEER_LIST com peers conhecidos ou LEADER_INFO se não for o líder.
    """
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


def request_file_index(ip: str, port: int) -> list:
    """
    Pede lista de arquivos a um peer.
    Usado pelo novo super nó para reconstruir o Redis.
    """
    resp = send_and_receive(ip, port, proto.build(proto.FILE_INDEX))
    if resp and resp.get('type') == proto.FILE_INDEX_RESP:
        return resp.get('files', [])
    return []


# ── heartbeat ─────────────────────────────────────────────────

def heartbeat(ip: str, port: int) -> bool:
    """Verifica se um peer está vivo. Retorna True se respondeu."""
    resp = send_and_receive(ip, port, proto.build(proto.HEARTBEAT))
    return resp is not None and resp.get('type') == proto.HEARTBEAT_ACK


# ── eleição ───────────────────────────────────────────────────

def send_election(ip: str, port: int, peer_name: str, uptime: float) -> bool:
    """
    Envia mensagem de eleição para um peer.
    Retorna True se recebeu OK (peer tem uptime maior e vai assumir).
    """
    resp = send_and_receive(
        ip, port,
        proto.build(proto.ELECTION, peer_name=peer_name, uptime=uptime)
    )
    return resp is not None and resp.get('type') == proto.ELECTION_OK


def broadcast_leader(peers: list, peer_name: str, ip: str, port: int):
    """
    Anuncia novo líder para todos os peers conhecidos.
    Peers que não responderem são ignorados — não bloqueante.
    """
    msg = proto.build(proto.LEADER, peer_name=peer_name,
                      ip_address=ip, port=port)
    for peer in peers:
        send_and_receive(peer['ip_address'], peer['port'], msg)


# ── download ──────────────────────────────────────────────────

def download_file(peer_name: str, ip: str, port: int,
                  checksum: str, expected_filename: str) -> str:
    """
    Baixa um arquivo diretamente de outro peer.

    Retorna:
        'completed'  → arquivo baixado e íntegro
        'failed'     → erro de conexão ou arquivo corrompido
        'not_found'  → peer não tem o arquivo
    """
    download_folder = Path(config.DOWNLOAD_FOLDER)
    download_folder.mkdir(parents=True, exist_ok=True)

    sock = _connect(ip, port)
    if not sock:
        return 'failed'

    try:
        # solicita o arquivo
        sock.sendall(proto.build(proto.DOWNLOAD_REQ, checksum=checksum))

        # recebe header
        header = proto.receive_message(sock)
        if not header:
            logger.error(f"Sem resposta ao DOWNLOAD_REQ de '{peer_name}'.")
            return 'failed'

        if header.get('type') == proto.ERROR:
            reason = header.get('reason')
            logger.warning(f"Peer '{peer_name}' recusou download: {reason}")
            return 'not_found' if reason == 'file_not_found' else 'failed'

        if header.get('type') != proto.FILE_DATA:
            logger.error(f"Resposta inesperada: {header.get('type')}")
            return 'failed'

        filename   = header.get('filename', expected_filename)
        total_size = int(header.get('size_bytes', 0))
        filepath   = download_folder / filename

        # recebe bytes
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

        # verifica integridade
        if bytes_recebidos < total_size:
            logger.error(f"Download incompleto: {bytes_recebidos}/{total_size} bytes.")
            _delete_file(filepath)
            return 'failed'

        if sha256.hexdigest() != checksum:
            logger.error(f"Checksum inválido para '{filename}'. Arquivo descartado.")
            _delete_file(filepath)
            return 'failed'

        logger.info(f"Download de '{filename}' concluído ({bytes_recebidos} bytes).")
        return 'completed'

    except socket.timeout:
        logger.error(f"Timeout no download de '{peer_name}'.")
        _delete_file(download_folder / expected_filename)
        return 'failed'
    except Exception as e:
        logger.error(f"Erro no download: {e}")
        _delete_file(download_folder / expected_filename)
        return 'failed'
    finally:
        sock.close()


def _delete_file(filepath: Path):
    try:
        if filepath.exists():
            filepath.unlink()
    except OSError:
        pass
