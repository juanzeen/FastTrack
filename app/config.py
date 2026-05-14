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
_raw = os.getenv('BOOTSTRAP_PEERS', '')
BOOTSTRAP_PEERS = []
for entry in _raw.split(','):
    entry = entry.strip()
    if ':' in entry:
        ip, port = entry.split(':')
        BOOTSTRAP_PEERS.append((ip.strip(), int(port.strip())))

# ── timeouts e intervalos ─────────────────────────────────────
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 10))  # segundos
SOCKET_TIMEOUT     = int(os.getenv('SOCKET_TIMEOUT',     5))
ELECTION_TIMEOUT   = int(os.getenv('ELECTION_TIMEOUT',   5))   # aguarda OK na eleição

# ── redis ─────────────────────────────────────────────────────
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_TTL  = int(os.getenv('REDIS_TTL', 30))
