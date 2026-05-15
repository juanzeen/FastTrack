import json

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
ANNOUNCE         = 'ANNOUNCE'       # peer anuncia presença + arquivos
PEER_LIST        = 'PEER_LIST'      # resposta com peers conhecidos
GET_PEERS        = 'GET_PEERS'      # request para pedir lista de peers
FILE_INDEX       = 'FILE_INDEX'     # super nó pede lista de arquivos
FILE_INDEX_RESP  = 'FILE_INDEX_RESP'

# download
DOWNLOAD_REQ     = 'DOWNLOAD_REQ'
FILE_DATA        = 'FILE_DATA'      # header antes dos bytes binários
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
MAX_HEADER = 4096  # bytes — proteção contra headers maliciosos


# ── build helpers ─────────────────────────────────────────────

def build(msg_type: str, **kwargs) -> bytes:
    """Serializa mensagem para bytes prontos para envio."""
    assert msg_type in ALLOWED, f"Tipo inválido: {msg_type}"
    msg = {'type': msg_type, **kwargs}
    return json.dumps(msg).encode('utf-8') + DELIMITER


def parse(raw: str) -> dict:
    """Desserializa string JSON recebida."""
    msg = json.loads(raw)
    if msg.get('type') not in ALLOWED:
        raise ValueError(f"Tipo de mensagem desconhecido: {msg.get('type')}")
    return msg


# ── receive helper ────────────────────────────────────────────

def receive_message(sock) -> dict | None:
    """
    Lê uma mensagem JSON delimitada por \\n do socket.
    Usa bytearray para evitar O(n²).
    Aborta se exceder MAX_HEADER bytes — proteção contra peer malicioso.
    Retorna None se a conexão fechar.
    """
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

        raise ValueError(f"Header excedeu {MAX_HEADER} bytes — conexão abortada.")

    except (ConnectionResetError, _socket.timeout):
        return None
