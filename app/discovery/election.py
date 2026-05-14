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

    Fluxo:
    1. Marca eleição em andamento
    2. Envia ELECTION para todos com uptime maior
    3. Se alguém responder OK → esse peer vai assumir, aguarda LEADER
    4. Se ninguém responder → se declara líder e notifica todos

    Retorna True se este peer se tornou líder.
    """
    if state.election_in_progress:
        logger.info("Eleição já em andamento, aguardando...")
        return False

    state.election_in_progress = True
    logger.info(f"Iniciando eleição. Uptime atual: {state.uptime:.1f}s")

    peers_acima = state.get_peers_with_higher_uptime()

    if not peers_acima:
        # ninguém com uptime maior → sou o líder
        _declare_leader(state)
        return True

    # envia ELECTION para todos com uptime maior
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
        # aguarda o LEADER chegar via server.py
        # se demorar demais, inicia nova eleição
        deadline = time.time() + config.ELECTION_TIMEOUT * 2
        while time.time() < deadline:
            if not state.election_in_progress:
                return False  # LEADER chegou, eleição resolvida
            time.sleep(0.5)

        # timeout — ninguém se declarou líder, assumimos
        logger.warning("Timeout aguardando LEADER. Assumindo liderança.")
        _declare_leader(state)
        return True

    else:
        # nenhum peer com uptime maior respondeu → sou o líder
        _declare_leader(state)
        return True


def _declare_leader(state):
    """Declara este peer como líder e notifica toda a rede."""
    logger.info(f"'{state.peer_name}' se declara líder (uptime: {state.uptime:.1f}s).")

    state.is_leader            = True
    state.election_in_progress = False
    state.current_leader       = state.to_dict()

    # inicializa Redis local como super nó
    from storage.redis_store import init_redis
    init_redis()

    # notifica todos os peers conhecidos
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
    e reconstrói o Redis do zero.
    """
    from storage.redis_store import register_peer, register_peer_files

    logger.info("Reconstruindo índice da rede...")
    peers = state.known_peers

    for peer in peers:
        files = client.request_file_index(peer['ip_address'], peer['port'])
        if files:
            register_peer(
                peer['peer_name'],
                peer['ip_address'],
                peer['port'],
                peer.get('uptime', 0)
            )
            register_peer_files(peer['peer_name'], files)
            logger.info(f"Índice de '{peer['peer_name']}' reconstruído ({len(files)} arquivo(s)).")

    logger.info("Reconstrução do índice concluída.")
