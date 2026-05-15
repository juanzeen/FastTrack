import threading
import logging
import time
from exceptions import StorageError

logger = logging.getLogger(__name__)

_lock = threading.RLock()

# Storage structures
_peers: dict = {}          # peer_name -> {ip_address, port, uptime, last_update}
_peer_files: dict = {}     # peer_name -> set of checksums
_file_meta: dict = {}      # checksum -> {filename, size_bytes}
_file_peers: dict = {}     # checksum -> set of peer_names

# Configuration
PEER_TTL = 30  # seconds — peers expire after this


def init_store():
    global _peers, _peer_files, _file_meta, _file_peers
    try:
        with _lock:
            _peers.clear()
            _peer_files.clear()
            _file_meta.clear()
            _file_peers.clear()
        logger.info("In-memory store initialized.")
        return True
    except Exception as e:
        logger.error(f"Falha ao inicializar armazenamento: {e}")
        return False


# ── peers ─────────────────────────────────────────────────────

def register_peer(peer_name: str, ip_address: str, port: int, uptime: float) -> bool:
    try:
        with _lock:
            _peers[peer_name] = {
                'ip_address': ip_address,
                'port': port,
                'uptime': uptime,
                'last_update': time.time()
            }
        return True
    except Exception as e:
        logger.error(f"Erro ao registrar peer '{peer_name}': {e}")
        return False


def get_peer_info(peer_name: str) -> dict | None:
    try:
        with _lock:
            if peer_name not in _peers:
                return None

            peer = _peers[peer_name]
            if time.time() - peer['last_update'] > PEER_TTL:
                del _peers[peer_name]
                return None

            return {
                'peer_name': peer_name,
                'ip_address': peer['ip_address'],
                'port': peer['port'],
                'uptime': peer['uptime']
            }
    except Exception as e:
        logger.error(f"Erro ao buscar info de '{peer_name}': {e}")
        return None


def get_all_peers() -> list[dict]:
    """Return all active peers (TTL not expired)."""
    try:
        with _lock:
            now = time.time()
            expired = [
                name for name, peer in _peers.items()
                if now - peer['last_update'] > PEER_TTL
            ]

            for name in expired:
                del _peers[name]

            return [
                {
                    'peer_name': name,
                    'ip_address': peer['ip_address'],
                    'port': peer['port'],
                    'uptime': peer['uptime']
                }
                for name, peer in _peers.items()
            ]
    except Exception as e:
        logger.error(f"Erro ao listar peers: {e}")
        return []


def remove_peer(peer_name: str) -> bool:
    """Remove a peer and clean up its files."""
    try:
        with _lock:
            if peer_name not in _peers:
                return True

            if peer_name in _peer_files:
                checksums = _peer_files[peer_name]
                for checksum in checksums:
                    if checksum in _file_peers:
                        _file_peers[checksum].discard(peer_name)
                        # If no peers have this file, remove file meta
                        if not _file_peers[checksum]:
                            if checksum in _file_peers: del _file_peers[checksum]
                            if checksum in _file_meta: del _file_meta[checksum]
                del _peer_files[peer_name]

            if peer_name in _peers:
                del _peers[peer_name]
        return True
    except Exception as e:
        logger.error(f"Erro ao remover peer '{peer_name}': {e}")
        return False


def refresh_all_ttls():
    try:
        with _lock:
            now = time.time()
            for peer in _peers.values():
                peer['last_update'] = now
    except Exception as e:
        logger.error(f"Erro ao renovar TTLs: {e}")


# ── arquivos ──────────────────────────────────────────────────

def register_peer_files(peer_name: str, files: list[dict]) -> bool:
    try:
        with _lock:
            new_checksums = {f['checksum'] for f in files}
            old_checksums = _peer_files.get(peer_name, set())
            removed = old_checksums - new_checksums

            # Remove old files
            for checksum in removed:
                if checksum in _file_peers:
                    _file_peers[checksum].discard(peer_name)
                    if not _file_peers[checksum]:
                        if checksum in _file_peers: del _file_peers[checksum]
                        if checksum in _file_meta: del _file_meta[checksum]

            # Add new files
            _peer_files[peer_name] = new_checksums
            for f in files:
                checksum = f['checksum']
                _file_meta[checksum] = {
                    'filename': f['filename'],
                    'size_bytes': f['size_bytes']
                }
                if checksum not in _file_peers:
                    _file_peers[checksum] = set()
                _file_peers[checksum].add(peer_name)

        return True
    except Exception as e:
        logger.error(f"Erro ao registrar arquivos de '{peer_name}': {e}")
        return False


def get_peers_with_file(checksum: str) -> list[dict]:
    """Return active peers that have the file."""
    try:
        with _lock:
            if checksum not in _file_peers:
                return []

            peer_names = _file_peers[checksum]
            peers = []

            for peer_name in peer_names:
                peer = get_peer_info(peer_name)
                if peer:
                    peers.append(peer)

            return peers
    except Exception as e:
        logger.error(f"Erro ao buscar peers com arquivo '{checksum}': {e}")
        return []


def get_file_meta(checksum: str) -> dict | None:
    """Get file metadata."""
    try:
        with _lock:
            return _file_meta.get(checksum)
    except Exception as e:
        logger.error(f"Erro ao buscar metadados do arquivo '{checksum}': {e}")
        return None


def search_file_by_name(query: str) -> list[dict]:
    """Search files by name (case-insensitive partial match)."""
    try:
        with _lock:
            results = []
            for checksum, meta in _file_meta.items():
                if query.lower() in meta.get('filename', '').lower():
                    peers = []
                    if checksum in _file_peers:
                        for peer_name in _file_peers[checksum]:
                            peer = get_peer_info(peer_name)
                            if peer:
                                peers.append(peer)

                    if peers:
                        results.append({
                            'checksum': checksum,
                            'filename': meta['filename'],
                            'size_bytes': meta['size_bytes'],
                            'peers': peers
                        })
            return results
    except Exception as e:
        logger.error(f"Erro ao buscar '{query}': {e}")
        return []


def get_peer_files(peer_name: str) -> list[dict]:
    """Get all files from a specific peer."""
    try:
        with _lock:
            if peer_name not in _peer_files:
                return []

            files = []
            for checksum in _peer_files[peer_name]:
                if checksum in _file_meta:
                    meta = _file_meta[checksum]
                    files.append({
                        'checksum': checksum,
                        'filename': meta['filename'],
                        'size_bytes': meta['size_bytes']
                    })
            return files
    except Exception as e:
        logger.error(f"Erro ao listar arquivos de '{peer_name}': {e}")
        return []
