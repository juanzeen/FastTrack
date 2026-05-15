import os
import socket
from dotenv import load_dotenv

load_dotenv()

# ── identidade do peer ────────────────────────────────────────
PEER_NAME = os.getenv('PEER_NAME', socket.gethostname())
PEER_PORT = int(os.getenv('PEER_PORT', 5000))

# ── pastas ────────────────────────────────────────────────────
SHARED_FOLDER   = os.getenv('SHARED_FOLDER',   './shared_files')
DOWNLOAD_FOLDER = os.getenv('DOWNLOAD_FOLDER', './downloads')

# ── rede ─────────────────────────────────────────────────────
# lista de IPs conhecidos para primeiro contato
# formato: "ip:porta,ip:porta"
# MUTÁVEL — peers descobertos são adicionados à lista
_raw = os.getenv('BOOTSTRAP_PEERS', '')
BOOTSTRAP_PEERS = []
for entry in _raw.split(','):
    entry = entry.strip()
    if ':' in entry:
        ip, port = entry.split(':')
        BOOTSTRAP_PEERS.append((ip.strip(), int(port.strip())))

# Lock para thread-safety ao adicionar/consultar bootstrap list
_bootstrap_lock = __import__('threading').Lock()


def add_to_bootstrap(ip: str, port: int) -> bool:
    """Add a peer to bootstrap list if not already there."""
    with _bootstrap_lock:
        if (ip, port) not in BOOTSTRAP_PEERS:
            BOOTSTRAP_PEERS.append((ip, port))
            return True
        return False


def get_bootstrap_peers() -> list:
    """Get current bootstrap list safely."""
    with _bootstrap_lock:
        return list(BOOTSTRAP_PEERS)

# ── timeouts e intervalos ─────────────────────────────────────
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 10))  # segundos
SOCKET_TIMEOUT     = int(os.getenv('SOCKET_TIMEOUT',     5))
ELECTION_TIMEOUT   = int(os.getenv('ELECTION_TIMEOUT',   5))   # aguarda OK na eleição

