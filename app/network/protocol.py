import json
import logging
from exceptions import ProtocolError

logger = logging.getLogger(__name__)

# ── tipos de mensagem ─────────────────────────────────────────
# sistema
WHO_IS_LEADER    = 'WHO_IS_LEADER'
LEADER_INFO      = 'LEADER_INFO'
ELECTION         = 'ELECTION'
ELECTION_OK      = 'ELECTION_OK'
LEADER           = 'LEADER'
HEARTBEAT        = 'HEARTBEAT'
HEARTBEAT_ACK    = 'HEARTBEAT_ACK'
ERROR            = 'ERROR'

# descoberta
ANNOUNCE         = 'ANNOUNCE'
PEER_LIST        = 'PEER_LIST'
GET_PEERS        = 'GET_PEERS'
FILE_INDEX       = 'FILE_INDEX'
FILE_INDEX_RESP  = 'FILE_INDEX_RESP'

# download
DOWNLOAD_REQ     = 'DOWNLOAD_REQ'
FILE_DATA        = 'FILE_DATA'
SEARCH_FILES_REQUEST = 'SEARCH_FILES_REQUEST'
SEARCH_FILES_RESPONSE = 'SEARCH_FILES_RESPONSE'
GET_FILE_SOURCES_REQUEST = 'GET_FILE_SOURCES_REQUEST'
GET_FILE_SOURCES_RESPONSE = 'GET_FILE_SOURCES_RESPONSE'

ALLOWED = {
    WHO_IS_LEADER, LEADER_INFO,
    ELECTION, ELECTION_OK, LEADER,
    HEARTBEAT, HEARTBEAT_ACK,
    ERROR,
    ANNOUNCE, PEER_LIST, GET_PEERS,
    FILE_INDEX, FILE_INDEX_RESP,
    DOWNLOAD_REQ, FILE_DATA,
    SEARCH_FILES_REQUEST, SEARCH_FILES_RESPONSE,
    GET_FILE_SOURCES_REQUEST, GET_FILE_SOURCES_RESPONSE,
}

# ── delimitador de mensagem ───────────────────────────────────
DELIMITER = b'\n'
MAX_HEADER = 4096


# ── build helpers ─────────────────────────────────────────────

def build(msg_type: str, **kwargs) -> bytes:
    if msg_type not in ALLOWED:
        raise ProtocolError(f"Tipo de mensagem inválido para build: {msg_type}")

    try:
        msg = {'type': msg_type, **kwargs}
        return json.dumps(msg).encode('utf-8') + DELIMITER
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"Erro ao serializar mensagem {msg_type}: {e}")


def parse(raw: str) -> dict:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ProtocolError(f"Erro ao decodificar JSON: {e}")

    if not isinstance(msg, dict):
        raise ProtocolError(f"Mensagem inválida (não é um dicionário): {type(msg)}")

    if msg.get('type') not in ALLOWED:
        raise ProtocolError(f"Tipo de mensagem desconhecido: {msg.get('type')}")

    return msg


# ── receive helper ────────────────────────────────────────────

def receive_message(sock) -> dict | None:
    import socket as _socket
    buf = bytearray()
    try:
        while len(buf) < MAX_HEADER:
            char = sock.recv(1)
            if not char:
                return None
            if char == DELIMITER:
                return parse(buf.decode('utf-8').strip())
            buf += char

        raise ProtocolError(f"Header excedeu {MAX_HEADER} bytes — conexão abortada.")

    except (ConnectionResetError, _socket.timeout, BrokenPipeError):
        return None
    except ProtocolError as e:
        logger.error(f"Erro de protocolo: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao receber mensagem: {e}")
        return None
