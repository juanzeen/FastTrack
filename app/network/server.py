import socket
import threading
import logging
import os

from network import protocol as proto
from files.manager import get_file_for_download, scan_shared_folder
import config

logger = logging.getLogger(__name__)


class PeerServer:
    """
    Servidor TCP do peer. Roda em thread separada e responde
    conexões de outros peers — downloads, heartbeats, eleições.
    Não faz login, registro ou qualquer operação local.
    """

    def __init__(self, peer_state):
        """
        peer_state: objeto compartilhado com o resto do sistema.
        Contém: peer_name, uptime, known_peers, current_leader,
                is_leader, election_in_progress.
        """
        self.state   = peer_state
        self._server = None
        self._thread = None
        self._running = False

    # ── ciclo de vida ─────────────────────────────────────────

    def start(self):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(('0.0.0.0', config.PEER_PORT))
        self._server.listen(10)
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        logger.info(f"Servidor TCP ouvindo na porta {config.PEER_PORT}.")

    def stop(self):
        self._running = False
        if self._server:
            self._server.close()

    # ── loop de aceitação ─────────────────────────────────────

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server.accept()
                t = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                )
                t.start()
            except OSError:
                break

    # ── handler de conexão ────────────────────────────────────

    def _handle_connection(self, conn, addr):
        conn.settimeout(config.SOCKET_TIMEOUT)
        try:
            msg = proto.receive_message(conn)
            if not msg:
                return

            t = msg.get('type')
            logger.debug(f"Recebido {t} de {addr}")

            if t == proto.WHO_IS_LEADER:
                self._handle_who_is_leader(conn)

            elif t == proto.ANNOUNCE:
                self._handle_announce(conn, msg)

            elif t == proto.FILE_INDEX:
                self._handle_file_index(conn)

            elif t == proto.HEARTBEAT:
                conn.sendall(proto.build(proto.HEARTBEAT_ACK))

            elif t == proto.ELECTION:
                self._handle_election(conn, msg)

            elif t == proto.LEADER:
                self._handle_leader(conn, msg)

            elif t == proto.DOWNLOAD_REQ:
                self._handle_download(conn, msg)

            elif t == proto.GET_PEERS:
                self._handle_get_peers(conn)

            else:
                conn.sendall(proto.build(proto.ERROR, reason='unsupported_message_type'))

        except Exception as e:
            logger.error(f"Erro ao processar conexão de {addr}: {e}")
        finally:
            conn.close()

    # ── handlers individuais ──────────────────────────────────

    def _handle_who_is_leader(self, conn):
        leader = self.state.current_leader
        conn.sendall(proto.build(
            proto.LEADER_INFO,
            leader=leader,
            election_in_progress=self.state.election_in_progress
        ))

    def _handle_announce(self, conn, msg):
        """
        Recebe anúncio de peer novo. Só o super nó processa de verdade.
        Peers comuns respondem com quem é o líder.
        """
        if not self.state.is_leader:
            conn.sendall(proto.build(
                proto.LEADER_INFO,
                leader=self.state.current_leader,
                election_in_progress=self.state.election_in_progress
            ))
            return

        peer_name  = msg.get('peer_name')
        ip_address = msg.get('ip_address')
        port       = msg.get('port')
        uptime     = msg.get('uptime', 0)
        files      = msg.get('files', [])

        # registra no storage em memória
        from storage.dict_store import register_peer, register_peer_files
        register_peer(peer_name, ip_address, port, uptime)
        register_peer_files(peer_name, files)

        # adiciona aos peers conhecidos em memória
        self.state.add_known_peer({
            'peer_name':  peer_name,
            'ip_address': ip_address,
            'port':       port,
            'uptime':     uptime
        })
        
        # Adiciona o novo peer à bootstrap list
        config.add_to_bootstrap(ip_address, port)

        # responde com lista de peers ativos
        from storage.dict_store import get_all_peers
        peers = get_all_peers()
        conn.sendall(proto.build(proto.PEER_LIST, peers=peers))

    def _handle_file_index(self, conn):
        files = scan_shared_folder()
        to_announce = [
            {'filename': f['filename'],
             'size_bytes': f['size_bytes'],
             'checksum': f['checksum']}
            for f in files
        ]
        conn.sendall(proto.build(proto.FILE_INDEX_RESP, files=to_announce))

    def _handle_get_peers(self, conn):
        peers = []
        if self.state.is_leader:
            from storage.dict_store import get_all_peers
            peers = get_all_peers()
            if not any(p['peer_name'] == self.state.peer_name for p in peers):
                peers.append(self.state.to_dict())
        else:
            peers = self.state.known_peers
            if self.state.current_leader and all(
                p['peer_name'] != self.state.current_leader['peer_name']
                for p in peers
            ):
                peers.append(self.state.current_leader)
        conn.sendall(proto.build(proto.PEER_LIST, peers=peers))

    def _handle_election(self, conn, msg):
        """
        Recebe mensagem de eleição. Se nosso uptime for maior,
        respondemos OK e iniciamos nossa própria eleição.
        """
        sender_uptime = msg.get('uptime', 0)
        sender_name   = msg.get('peer_name')

        if self.state.uptime > sender_uptime:
            conn.sendall(proto.build(proto.ELECTION_OK))
            # inicia eleição própria se não houver uma em andamento
            if not self.state.election_in_progress:
                from discovery.election import start_election
                threading.Thread(
                    target=start_election,
                    args=(self.state,),
                    daemon=True
                ).start()
        else:
            conn.sendall(proto.build(proto.ERROR, reason='lower_uptime'))

    def _handle_leader(self, conn, msg):
        """Recebe anúncio de novo líder e atualiza estado local."""
        self.state.current_leader = {
            'peer_name':  msg.get('peer_name'),
            'ip_address': msg.get('ip_address'),
            'port':       msg.get('port')
        }
        self.state.is_leader           = (msg.get('peer_name') == config.PEER_NAME)
        self.state.election_in_progress = False
        conn.sendall(proto.build(proto.HEARTBEAT_ACK))
        logger.info(f"Novo líder: {msg.get('peer_name')}")

    def _handle_download(self, conn, msg):
        """
        Serve um arquivo para outro peer via TCP.
        Fluxo: header JSON → bytes binários.
        """
        checksum = msg.get('checksum')
        if not checksum:
            conn.sendall(proto.build(proto.ERROR, reason='missing_checksum'))
            return

        arquivo = get_file_for_download(checksum, config.PEER_NAME)
        if not arquivo:
            conn.sendall(proto.build(proto.ERROR, reason='file_not_found'))
            return

        # envia header com metadados
        conn.sendall(proto.build(
            proto.FILE_DATA,
            filename=arquivo['filename'],
            size_bytes=arquivo['size_bytes'],
            checksum=arquivo['checksum']
        ))

        # envia bytes do arquivo
        try:
            with open(arquivo['filepath'], 'rb') as f:
                while chunk := f.read(4096):
                    conn.sendall(chunk)
        except OSError as e:
            logger.error(f"Erro ao enviar arquivo '{arquivo['filename']}': {e}")
