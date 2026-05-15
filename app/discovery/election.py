import threading
import logging
import time
import config
from network import client

logger = logging.getLogger(__name__)


def start_election(state) -> bool:
    """
    Algoritmo Bully baseado em uptime.
    Peer com maior uptime vira líder — quem está há mais tempo
    online conhece mais a rede.
    """
    if state.election_in_progress:
        logger.info("Eleição já em andamento, aguardando...")
        return False

    state.election_in_progress = True
    logger.info(f"Iniciando eleição. Uptime atual: {state.uptime:.1f}s")

    peers_acima = state.get_peers_with_higher_uptime()

    if not peers_acima:
        _declare_leader(state)
        return True

    got_ok = False
    for peer in peers_acima:
        ok = client.send_election(
            peer['ip_address'],
            peer['port'],
            state.peer_name,
            state.uptime
        )
        if ok:
            got_ok = True
            logger.info(f"Recebeu OK de '{peer['peer_name']}' — ele vai assumir.")
            break

    if got_ok:
        deadline = time.time() + config.ELECTION_TIMEOUT * 2
        while time.time() < deadline:
            if not state.election_in_progress:
                return False
            time.sleep(0.5)

        logger.warning("Timeout aguardando LEADER. Assumindo liderança.")
        _declare_leader(state)
        return True

    else:
        _declare_leader(state)
        return True


def _declare_leader(state):
    logger.info(f"'{state.peer_name}' se declara líder (uptime: {state.uptime:.1f}s).")

    state.is_leader            = True
    state.election_in_progress = False
    state.current_leader       = state.to_dict()

    from storage.dict_store import init_store, register_peer, register_peer_files
    from files.manager import scan_shared_folder

    init_store()
    register_peer(state.peer_name, state.ip_address, state.port, state.uptime)

    files = scan_shared_folder()
    to_announce = [
        {'filename': f['filename'],
         'size_bytes': f['size_bytes'],
         'checksum': f['checksum']}
        for f in files
    ]
    register_peer_files(state.peer_name, to_announce)

    client.broadcast_leader(
        state.known_peers,
        state.peer_name,
        state.ip_address,
        state.port
    )

    # reconstrói índice pedindo FILE_INDEX para cada peer
    _rebuild_index(state)


def _rebuild_index(state):
    """
    Novo super nó pede FILE_INDEX para todos os peers conhecidos
    e reconstrói o storage do zero.
    """
    from storage.dict_store import register_peer, register_peer_files

    logger.info("Reconstruindo índice da rede...")
    peers = state.known_peers

    for peer in peers:
        if peer['peer_name'] == state.peer_name:
            continue

        files = client.request_file_index(peer['ip_address'], peer['port'])
        # É vital registrar o peer ANTES dos arquivos para que o TTL seja válido
        register_peer(
            peer['peer_name'],
            peer['ip_address'],
            peer['port'],
            peer.get('uptime', 0)
        )
        if files:
            register_peer_files(peer['peer_name'], files)
            logger.info(f"Índice de '{peer['peer_name']}' reconstruído ({len(files)} arquivo(s)).")
        else:
            logger.warning(f"Peer '{peer['peer_name']}' não retornou arquivos ou está offline.")

    logger.info("Reconstrução do índice concluída.")
