import redis
import logging
from dotenv import load_dotenv
import os
 
load_dotenv()
 
logger = logging.getLogger(__name__)

PEER_TTL = int(os.getenv('REDIS_PEER_TTL', 30))

def get_redis():
    return redis.Redis(
        host='localhost',
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=0,
        decode_responses=True
    )

def init_redis():
    try:
        r = get_redis()
        r.ping()
        logger.info("Redis conectado com sucesso.")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Erro ao conectar no Redis: {e}")
        return False

# peer:{peer_name}:files - Set com checksums dos arquivos do peer
# peer:{peer_name}:info  - Hash com ip e porta do peer
# file:{checksum}:meta   - Hash com filename e size_bytes
# file:{checksum}:peers  - Set com peer_names que têm esse arquivo
# leader                 - String com peer_name do super nó atual

def register_peer(peer_name, ip_address, port):
    try:
        r = get_redis()
        pipe = r.pipeline()
 
        pipe.hset(f"peer:{peer_name}:info", mapping={
            "ip_address": ip_address,
            "port": port
        })
        pipe.expire(f"peer:{peer_name}:info", PEER_TTL)
 
        pipe.execute()
        logger.info(f"Peer '{peer_name}' registrado no Redis.")
        return True
 
    except Exception as e:
        logger.error(f"Erro ao registrar peer '{peer_name}' no Redis: {e}")
        return False

def refresh_peer_ttl(peer_name):
    try:
        r = get_redis()
        pipe = r.pipeline()
 
        pipe.expire(f"peer:{peer_name}:info", PEER_TTL)
        pipe.expire(f"peer:{peer_name}:files", PEER_TTL)
 
        pipe.execute()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao renovar TTL do peer '{peer_name}': {e}")
        return False

def get_peer_info(peer_name):
    try:
        r = get_redis()
        info = r.hgetall(f"peer:{peer_name}:info")
 
        if not info:
            logger.warning(f"Peer '{peer_name}' não encontrado no Redis (pode ter expirado).")
            return None
 
        return {
            "peer_name": peer_name,
            "ip_address": info["ip_address"],
            "port": int(info["port"])
        }
 
    except Exception as e:
        logger.error(f"Erro ao buscar info do peer '{peer_name}': {e}")
        return None

def remove_peer(peer_name):
    try:
        r = get_redis()
 
        checksums = r.smembers(f"peer:{peer_name}:files")
 
        pipe = r.pipeline()
 
        for checksum in checksums:
            pipe.srem(f"file:{checksum}:peers", peer_name)

        pipe.delete(f"peer:{peer_name}:files")
        pipe.delete(f"peer:{peer_name}:info")
 
        pipe.execute()
 
        for checksum in checksums:
            if r.scard(f"file:{checksum}:peers") == 0:
                r.delete(f"file:{checksum}:meta")
                r.delete(f"file:{checksum}:peers")
 
        logger.info(f"Peer '{peer_name}' removido do Redis.")
        return True
 
    except Exception as e:
        logger.error(f"Erro ao remover peer '{peer_name}' do Redis: {e}")
        return False

def register_peer_files(peer_name, files):
    try:
        r = get_redis()

        novos_checksums = {f["checksum"] for f in files}
        checksums_antigos = r.smembers(f"peer:{peer_name}:files")
        removidos = checksums_antigos - novos_checksums

        pipe = r.pipeline()

        for checksum in removidos:
            pipe.srem(f"peer:{peer_name}:files", checksum)
            pipe.srem(f"file:{checksum}:peers", peer_name)

        for file in files:
            checksum   = file["checksum"]
            filename   = file["filename"]
            size_bytes = file["size_bytes"]

            pipe.sadd(f"peer:{peer_name}:files", checksum)
            pipe.hset(f"file:{checksum}:meta", mapping={
                "filename":   filename,
                "size_bytes": size_bytes
            })
            pipe.sadd(f"file:{checksum}:peers", peer_name)
            pipe.expire(f"file:{checksum}:meta", PEER_TTL)

        pipe.expire(f"peer:{peer_name}:files", PEER_TTL)
        pipe.execute()

        for checksum in removidos:
            if r.scard(f"file:{checksum}:peers") == 0:
                r.delete(f"file:{checksum}:meta")

        logger.info(f"Arquivos do peer '{peer_name}' sincronizados ({len(files)} arquivo(s), {len(removidos)} removido(s)).")
        return True

    except Exception as e:
        logger.error(f"Erro ao registrar arquivos do peer '{peer_name}': {e}")
        return False

def get_peers_with_file(checksum):
    try:
        r = get_redis()
        peers = r.smembers(f"file:{checksum}:peers")

        peers_ativos = [
            p for p in peers
            if r.exists(f"peer:{p}:info")
        ]

        zumbis = peers - set(peers_ativos)
        if zumbis:
            pipe = r.pipeline()
            for zumbi in zumbis:
                pipe.srem(f"file:{checksum}:peers", zumbi)
            pipe.execute()
            logger.info(f"Removidos {len(zumbis)} peer(s) zumbi(s) do arquivo '{checksum}'.")

        return peers_ativos

    except Exception as e:
        logger.error(f"Erro ao buscar peers com arquivo '{checksum}': {e}")
        return []
    
def get_file_meta(checksum):
    try:
        r = get_redis()
        meta = r.hgetall(f"file:{checksum}:meta")
 
        if not meta:
            return None
 
        return {
            "checksum":   checksum,
            "filename":   meta["filename"],
            "size_bytes": int(meta["size_bytes"])
        }
 
    except Exception as e:
        logger.error(f"Erro ao buscar metadados do arquivo '{checksum}': {e}")
        return None

def get_peer_files(peer_name):
    try:
        r = get_redis()
        checksums = r.smembers(f"peer:{peer_name}:files")
 
        if not checksums:
            logger.warning(f"Nenhum arquivo encontrado para o peer '{peer_name}'.")
            return []
 
        files = []
        for checksum in checksums:
            meta = get_file_meta(checksum)
            if meta:
                files.append(meta)
 
        return files
 
    except Exception as e:
        logger.error(f"Erro ao buscar arquivos do peer '{peer_name}': {e}")
        return []

def search_file_by_name(filename):
    try:
        r = get_redis()
        results = []
 
        for key in r.scan_iter("file:*:meta"):
            meta = r.hgetall(key)
 
            if not meta:
                continue
 
            if filename.lower() in meta.get("filename", "").lower():
                checksum = key.split(":")[1]
                peers    = list(r.smembers(f"file:{checksum}:peers"))
 
                results.append({
                    "checksum":   checksum,
                    "filename":   meta["filename"],
                    "size_bytes": int(meta["size_bytes"]),
                    "peers":      peers
                })
 
        return results
 
    except Exception as e:
        logger.error(f"Erro ao buscar arquivo por nome '{filename}': {e}")
        return []

def set_leader(peer_name):
    try:
        r = get_redis()
        r.set("leader", peer_name)
        logger.info(f"Super nó definido: '{peer_name}'.")
        return True
 
    except Exception as e:
        logger.error(f"Erro ao definir líder '{peer_name}': {e}")
        return False

def get_leader():
    try:
        r = get_redis()
        leader = r.get("leader")
 
        if not leader:
            logger.warning("Nenhum super nó definido. Eleição necessária.")
 
        return leader
 
    except Exception as e:
        logger.error(f"Erro ao buscar líder: {e}")
        return None

def clear_leader():
    try:
        r = get_redis()
        r.delete("leader")
        logger.info("Super nó removido. Eleição necessária.")
        return True
 
    except Exception as e:
        logger.error(f"Erro ao remover líder: {e}")
        return False