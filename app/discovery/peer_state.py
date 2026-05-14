import threading
import time
import socket
import logging
import config

logger = logging.getLogger(__name__)


class PeerState:
    """
    Estado compartilhado do peer em memória.
    Thread-safe via lock.
    Centraliza tudo que o sistema precisa saber sobre si mesmo e a rede.
    """

    def __init__(self):
        self._lock = threading.Lock()

        # identidade
        self.peer_name  = config.PEER_NAME
        self.ip_address = self._get_local_ip()
        self.port       = config.PEER_PORT
        self.started_at = time.time()

        # rede
        self._known_peers: list[dict] = []   # peers conhecidos em memória
        self._current_leader: dict | None = None

        # flags
        self.is_leader             = False
        self.election_in_progress  = False

    # ── uptime ────────────────────────────────────────────────

    @property
    def uptime(self) -> float:
        """
        Tempo em segundos desde que o peer iniciou.
        Usado como critério de eleição — maior uptime vira líder.
        Peer com mais tempo online conhece mais a rede.
        """
        return time.time() - self.started_at

    # ── current_leader ────────────────────────────────────────

    @property
    def current_leader(self) -> dict | None:
        with self._lock:
            return self._current_leader

    @current_leader.setter
    def current_leader(self, value: dict | None):
        with self._lock:
            self._current_leader = value

    # ── known_peers ───────────────────────────────────────────

    @property
    def known_peers(self) -> list[dict]:
        with self._lock:
            return list(self._known_peers)

    def add_known_peer(self, peer: dict):
        """Adiciona peer à lista local. Ignora se já existir pelo nome."""
        with self._lock:
            names = {p['peer_name'] for p in self._known_peers}
            if peer['peer_name'] not in names:
                self._known_peers.append(peer)

    def update_known_peers(self, peers: list[dict]):
        """
        Atualiza lista de peers conhecidos com a lista recebida do super nó.
        Faz merge — não substitui peers que já existem localmente.
        """
        with self._lock:
            names = {p['peer_name'] for p in self._known_peers}
            for peer in peers:
                if peer['peer_name'] not in names and peer['peer_name'] != self.peer_name:
                    self._known_peers.append(peer)

    def remove_known_peer(self, peer_name: str):
        with self._lock:
            self._known_peers = [
                p for p in self._known_peers
                if p['peer_name'] != peer_name
            ]

    def get_peers_with_higher_uptime(self) -> list[dict]:
        """Retorna peers com uptime maior que o nosso. Usado na eleição."""
        with self._lock:
            return [
                p for p in self._known_peers
                if p.get('uptime', 0) > self.uptime
            ]

    # ── helpers ───────────────────────────────────────────────

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    def to_dict(self) -> dict:
        return {
            'peer_name':  self.peer_name,
            'ip_address': self.ip_address,
            'port':       self.port,
            'uptime':     self.uptime
        }
