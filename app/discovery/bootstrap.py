import logging
import time
import config
from network import client
from files.manager import scan_shared_folder

logger = logging.getLogger(__name__)


def join_network(state) -> bool:
    """
    Processo completo de entrada na rede.
    Tenta cada IP da bootstrap list até encontrar alguém.

    Retorna True se entrou na rede, False se ficou isolado.
    """
    if not config.BOOTSTRAP_PEERS:
        logger.warning("Bootstrap list vazia. Iniciando como primeiro peer da rede.")
        _become_first_leader(state)
        return True

    # tenta cada peer da bootstrap list
    for ip, port in config.BOOTSTRAP_PEERS:

        # pula a si mesmo
        if ip == state.ip_address and port == state.port:
            continue

        logger.info(f"Tentando bootstrap em {ip}:{port}...")
        resp = client.who_is_leader(ip, port)

        if not resp:
            logger.warning(f"{ip}:{port} não respondeu.")
            continue

        # eleição em andamento — aguarda e tenta de novo
        if resp.get('election_in_progress'):
            logger.info("Eleição em andamento na rede. Aguardando...")
            time.sleep(config.ELECTION_TIMEOUT)
            resp = client.who_is_leader(ip, port)

        leader = resp.get('leader') if resp else None

        if not leader:
            logger.warning(f"{ip}:{port} não conhece líder. Tentando próximo...")
            continue

        # encontrou o líder — anuncia presença
        logger.info(f"Líder encontrado: {leader['peer_name']} ({leader['ip_address']}:{leader['port']})")
        success = _announce_to_leader(state, leader)

        if success:
            return True

    # nenhum peer da bootstrap list respondeu com líder
    # verifica se algum peer está vivo mas sem líder
    for ip, port in config.BOOTSTRAP_PEERS:
        if ip == state.ip_address and port == state.port:
            continue
        resp = client.who_is_leader(ip, port)
        if resp:
            # alguém está vivo mas sem líder — inicia eleição
            logger.info("Peers ativos sem líder. Iniciando eleição.")
            state.add_known_peer({
                'peer_name':  resp.get('peer_name', f'{ip}:{port}'),
                'ip_address': ip,
                'port':       port,
                'uptime':     resp.get('uptime', 0)
            })
            from discovery.election import start_election
            start_election(state)
            return True

    # absolutamente ninguém respondeu — primeiro da rede
    logger.info("Nenhum peer respondeu. Iniciando como primeiro peer.")
    _become_first_leader(state)
    return True


def _announce_to_leader(state, leader: dict) -> bool:
    """
    Anuncia presença ao super nó com lista de arquivos.
    Recebe de volta a lista de peers conhecidos.
    """
    files = scan_shared_folder()
    to_announce = [
        {'filename': f['filename'],
         'size_bytes': f['size_bytes'],
         'checksum': f['checksum']}
        for f in files
    ]

    resp = client.announce(
        leader['ip_address'],
        leader['port'],
        state.peer_name,
        state.ip_address,
        state.port,
        state.uptime,
        to_announce
    )

    if not resp:
        logger.error(f"Sem resposta do líder '{leader['peer_name']}' ao anúncio.")
        return False

    # líder respondeu com lista de peers
    if resp.get('type') == 'PEER_LIST':
        peers = resp.get('peers', [])
        state.update_known_peers(peers)
        state.current_leader = leader
        logger.info(f"Entrou na rede. {len(peers)} peer(s) conhecido(s).")
        return True

    # peer não era o líder — ele nos redirecionou
    if resp.get('type') == 'LEADER_INFO':
        new_leader = resp.get('leader')
        if new_leader:
            return _announce_to_leader(state, new_leader)

    return False


def _become_first_leader(state):
    """Primeiro peer da rede se declara líder imediatamente."""
    from storage.redis_store import init_redis
    init_redis()
    state.is_leader            = True
    state.election_in_progress = False
    state.current_leader       = state.to_dict()
    logger.info(f"'{state.peer_name}' é o primeiro peer — líder definido.")
