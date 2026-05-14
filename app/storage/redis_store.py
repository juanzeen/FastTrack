import redis
import logging
import config

logger = logging.getLogger(__name__)

PEER_TTL = config.REDIS_TTL

_client = None


def get_redis():
    global _client
    if _client is None:
        _client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=0,
            decode_responses=True
        )
    return _client


def init_redis():
    """Testa conexão. Chamado quando o peer se torna super nó."""
    try:
        get_redis().ping()
        logger.info("Redis conectado.")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis indisponível: {e}")
        return False


# ── peers ─────────────────────────────────────────────────────

def register_peer(peer_name: str, ip_address: str,
                  port: int, uptime: float) -> bool:
    try:
        r = get_redis()
        pipe = r.pipeline()
        pipe.hset(f"peer:{peer_name}:info", mapping={
            'ip_address': ip_address,
            'port':       port,
            'uptime':     uptime
        })
        pipe.expire(f"peer:{peer_name}:info", PEER_TTL)
        pipe.execute()
        return True
    except Exception as e:
        logger.error(f"Erro ao registrar peer '{peer_name}': {e}")
        return False


def get_peer_info(peer_name: str) -> dict | None:
    try:
        r = get_redis()
        info = r.hgetall(f"peer:{peer_name}:info")
        if not info:
            return None
        return {
            'peer_name':  peer_name,
            'ip_address': info['ip_address'],
            'port':       int(info['port']),
            'uptime':     float(info.get('uptime', 0))
        }
    except Exception as e:
        logger.error(f"Erro ao buscar info de '{peer_name}': {e}")
        return None


def get_all_peers() -> list[dict]:
    """Retorna todos os peers ativos no Redis (TTL não expirado)."""
    try:
        r = get_redis()
        peers = []
        for key in r.scan_iter("peer:*:info"):
            info = r.hgetall(key)
            if info:
                peer_name = key.split(':')[1]
                peers.append({
                    'peer_name':  peer_name,
                    'ip_address': info['ip_address'],
                    'port':       int(info['port']),
                    'uptime':     float(info.get('uptime', 0))
                })
        return peers
    except Exception as e:
        logger.error(f"Erro ao listar peers: {e}")
        return []


def remove_peer(peer_name: str) -> bool:
    try:
        r = get_redis()
        checksums = r.smembers(f"peer:{peer_name}:files")
        pipe = r.pipeline()

        for checksum in checksums:
            pipe.srem(f"file:{checksum}:peers", peer_name)

        pipe.delete(f"peer:{peer_name}:files")
        pipe.delete(f"peer:{peer_name}:info")
        pipe.execute()

        # limpa metadados órfãos
        for checksum in checksums:
            if r.scard(f"file:{checksum}:peers") == 0:
                r.delete(f"file:{checksum}:meta")
                r.delete(f"file:{checksum}:peers")

        return True
    except Exception as e:
        logger.error(f"Erro ao remover peer '{peer_name}': {e}")
        return False


def refresh_all_ttls():
    """
    Renova TTL de todos os peers e arquivos ativos.
    Chamado pelo super nó a cada ciclo de heartbeat.
    """
    try:
        r = get_redis()
        pipe = r.pipeline()
        for key in r.scan_iter("peer:*"):
            pipe.expire(key, PEER_TTL)
        for key in r.scan_iter("file:*:meta"):
            pipe.expire(key, PEER_TTL)
        pipe.execute()
    except Exception as e:
        logger.error(f"Erro ao renovar TTLs: {e}")


# ── arquivos ──────────────────────────────────────────────────

def register_peer_files(peer_name: str, files: list[dict]) -> bool:
    """
    Registra arquivos anunciados por um peer.
    Faz diff para remover checksums que o peer não tem mais.
    """
    try:
        r = get_redis()

        novos_checksums   = {f['checksum'] for f in files}
        antigos_checksums = r.smembers(f"peer:{peer_name}:files")
        removidos         = antigos_checksums - novos_checksums

        pipe = r.pipeline()

        for checksum in removidos:
            pipe.srem(f"peer:{peer_name}:files", checksum)
            pipe.srem(f"file:{checksum}:peers", peer_name)

        for f in files:
            checksum = f['checksum']
            pipe.sadd(f"peer:{peer_name}:files", checksum)
            pipe.hset(f"file:{checksum}:meta", mapping={
                'filename':   f['filename'],
                'size_bytes': f['size_bytes']
            })
            pipe.sadd(f"file:{checksum}:peers", peer_name)
            pipe.expire(f"file:{checksum}:meta", PEER_TTL)

        pipe.expire(f"peer:{peer_name}:files", PEER_TTL)
        pipe.execute()

        for checksum in removidos:
            if r.scard(f"file:{checksum}:peers") == 0:
                r.delete(f"file:{checksum}:meta")
                r.delete(f"file:{checksum}:peers")

        return True
    except Exception as e:
        logger.error(f"Erro ao registrar arquivos de '{peer_name}': {e}")
        return False


def get_peers_with_file(checksum: str) -> list[dict]:
    """
    Retorna peers ativos que têm o arquivo.
    Filtra zumbis — peers cujo TTL já expirou.
    """
    try:
        r = get_redis()
        peers = r.smembers(f"file:{checksum}:peers")

        ativos  = []
        zumbis  = []

        for p in peers:
            info = get_peer_info(p)
            if info:
                ativos.append(info)
            else:
                zumbis.append(p)

        if zumbis:
            pipe = r.pipeline()
            for z in zumbis:
                pipe.srem(f"file:{checksum}:peers", z)
            pipe.execute()

        return ativos
    except Exception as e:
        logger.error(f"Erro ao buscar peers com arquivo '{checksum}': {e}")
        return []


def search_file_by_name(query: str) -> list[dict]:
    """Busca arquivos por nome parcial, case-insensitive."""
    try:
        r = get_redis()
        results = []

        for key in r.scan_iter("file:*:meta"):
            meta = r.hgetall(key)
            if not meta:
                continue
            if query.lower() in meta.get('filename', '').lower():
                checksum = key.split(':')[1]
                peers    = get_peers_with_file(checksum)
                if peers:
                    results.append({
                        'checksum':   checksum,
                        'filename':   meta['filename'],
                        'size_bytes': int(meta['size_bytes']),
                        'peers':      peers
                    })

        return results
    except Exception as e:
        logger.error(f"Erro ao buscar '{query}': {e}")
        return []


def get_peer_files(peer_name: str) -> list[dict]:
    try:
        r = get_redis()
        checksums = r.smembers(f"peer:{peer_name}:files")
        files = []
        for checksum in checksums:
            meta = r.hgetall(f"file:{checksum}:meta")
            if meta:
                files.append({
                    'checksum':   checksum,
                    'filename':   meta['filename'],
                    'size_bytes': int(meta['size_bytes'])
                })
        return files
    except Exception as e:
        logger.error(f"Erro ao listar arquivos de '{peer_name}': {e}")
        return []
