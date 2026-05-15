import threading
import logging
import time
import config
from network import client

logger = logging.getLogger(__name__)


class HeartbeatService:
    def __init__(self, state):
        self.state    = state
        self._running = False
        self._thread  = None

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Heartbeat iniciado.")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(config.HEARTBEAT_INTERVAL)
            try:
                self._tick()
            except Exception as e:
                logger.error(f"Erro no heartbeat: {e}")

    def _tick(self):
        if self.state.is_leader:
            self._renew_peer_ttls()
            return

        # se não sou o líder, verifico se o líder está vivo
        leader = self.state.current_leader
        if not leader:
            logger.warning("Sem líder conhecido. Iniciando eleição.")
            self._trigger_election()
            return

        alive = client.heartbeat(leader['ip_address'], leader['port'])

        if alive:
            logger.debug(f"Líder '{leader['peer_name']}' respondeu heartbeat.")
        else:
            logger.warning(
                f"Líder '{leader['peer_name']}' não respondeu. "
                f"Iniciando eleição."
            )
            self.state.current_leader = None
            self._trigger_election()

    def _trigger_election(self):
        from discovery.election import start_election
        threading.Thread(
            target=start_election,
            args=(self.state,),
            daemon=True
        ).start()

    def _renew_peer_ttls(self):
        from storage.dict_store import get_all_peers, remove_peer

        peers = get_all_peers()
        for peer in peers:
            if peer['peer_name'] == self.state.peer_name:
                continue

            alive = client.heartbeat(peer['ip_address'], peer['port'])
            if not alive:
                logger.warning(f"Peer '{peer['peer_name']}' parado ou inacessível. Removendo.")
                remove_peer(peer['peer_name'])
                self.state.remove_known_peer(peer['peer_name'])
            else:
                from storage.dict_store import register_peer
                register_peer(peer['peer_name'], peer['ip_address'], peer['port'], peer.get('uptime', 0))
