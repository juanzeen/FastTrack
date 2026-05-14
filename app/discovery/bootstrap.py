import logging
import time
import config
from network import client
from files.manager import scan_shared_folder

logger = logging.getLogger(__name__)


def join_network(state) -> bool:
    if not config.BOOTSTRAP_PEERS:
        logger.warning("Bootstrap list vazia. Tentando descoberta local...")
        if _discover_local_peers(state):
            return True
        logger.warning("Nenhum peer encontrado localmente. Iniciando como primeiro peer da rede.")
        _become_first_leader(state)
        return True

    for ip, port in config.BOOTSTRAP_PEERS:

        if ip == state.ip_address and port == state.port:
            continue

        logger.info(f"Tentando bootstrap em {ip}:{port}...")
        resp = client.who_is_leader(ip, port)

        if not resp:
            logger.warning(f"{ip}:{port} não respondeu.")
            continue

        if resp.get('election_in_progress'):
            logger.info("Eleição em andamento na rede. Aguardando...")
            time.sleep(config.ELECTION_TIMEOUT)
            resp = client.who_is_leader(ip, port)

        leader = resp.get('leader') if resp else None

        if not leader:
            logger.warning(f"{ip}:{port} não conhece líder. Tentando próximo...")
            continue

        logger.info(f"Líder encontrado: {leader['peer_name']} ({leader['ip_address']}:{leader['port']})")
        success = _announce_to_leader(state, leader)

        if success:
            return True

    for ip, port in config.BOOTSTRAP_PEERS:
        if ip == state.ip_address and port == state.port:
            continue
        resp = client.who_is_leader(ip, port)
        if resp:
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

    logger.info("Nenhum peer respondeu. Iniciando como primeiro peer.")
    _become_first_leader(state)
    return True


def _discover_local_peers(state) -> bool:
    ports_to_try = range(5000, 5010)
    ips_to_try = ['127.0.0.1', '::1', state.ip_address]
    
    for port in ports_to_try:
        if port == state.port:
            continue
        
        for ip in ips_to_try:
            logger.debug(f"Tentando descoberta local em {ip}:{port}...")
            resp = client.who_is_leader(ip, port)
            
            if resp:
                logger.info(f"Peer descoberto localmente em {ip}:{port}")
                leader = resp.get('leader')
                
                if leader and leader['peer_name'] != state.peer_name:
                    success = _announce_to_leader(state, leader)
                    if success:
                        return True
                elif resp.get('peer_name') and resp['peer_name'] != state.peer_name:
                    logger.info("Peer ativo encontrado, iniciando eleição.")
                    state.add_known_peer({
                        'peer_name': resp.get('peer_name'),
                        'ip_address': ip,
                        'port': port,
                        'uptime': resp.get('uptime', 0)
                    })
                    from discovery.election import start_election
                    start_election(state)
                    return True
    
    return False


def _announce_to_leader(state, leader: dict) -> bool:
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

    if resp.get('type') == 'PEER_LIST':
        peers = resp.get('peers', [])
        state.update_known_peers(peers)
        state.current_leader = leader
        
        config.add_to_bootstrap(leader['ip_address'], leader['port'])
        
        for peer in peers:
            config.add_to_bootstrap(peer['ip_address'], peer['port'])
        
        logger.info(f"Entrou na rede. {len(peers)} peer(s) conhecido(s).")
        return True

    if resp.get('type') == 'LEADER_INFO':
        new_leader = resp.get('leader')
        if new_leader:
            return _announce_to_leader(state, new_leader)

    return False


def _become_first_leader(state):
    from storage.dict_store import init_store, register_peer
    init_store()
    register_peer(state.peer_name, state.ip_address, state.port, state.uptime)
    state.is_leader            = True
    state.election_in_progress = False
    state.current_leader       = state.to_dict()
    logger.info(f"'{state.peer_name}' é o primeiro peer — líder definido.")
