import threading
import logging
import time
import config
from network import client

logger = logging.getLogger(__name__)


class HeartbeatService:
    """
    Roda em thread separada e monitora continuamente:
    1. O super nó atual — se cair, inicia eleição
    2. Renova TTL no Redis se este peer for o super nó
    """

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
        # se sou o líder, apenas renovo TTLs no Redis
        if self.state.is_leader:
            self._renew_redis_ttls()
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

    def _renew_redis_ttls(self):
        """
        Renova TTL de todos os peers ativos no Redis.
        Chamado pelo super nó a cada ciclo de heartbeat.
        """
        from storage.redis_store import refresh_all_ttls
        refresh_all_ttls()
