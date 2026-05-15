import socket
import threading
import logging
import os

from network import protocol as proto
from files.manager import get_file_for_download, scan_shared_folder
from exceptions import ProtocolError, NetworkError
import config

logger = logging.getLogger(__name__)


class PeerServer:
    def __init__(self, peer_state):
        self.state   = peer_state
        self._server = None
        self._thread = None
        self._running = False

    # ── ciclo de vida ─────────────────────────────────────────

    def start(self):
        try:
            self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server.bind(('0.0.0.0', config.PEER_PORT))
            self._server.listen(10)
            self._running = True
            self._thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._thread.start()
            logger.info(f"Servidor TCP ouvindo na porta {config.PEER_PORT}.")
        except Exception as e:
            logger.critical(f"Falha ao iniciar o servidor TCP: {e}")
            raise NetworkError(f"Não foi possível iniciar o servidor na porta {config.PEER_PORT}")

    def stop(self):
        self._running = False
        if self._server:
            try:
                self._server.close()
            except Exception:
                pass

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
                if self._running:
                    logger.error("Erro no accept loop do servidor.")
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
                self._handle_file_index(conn, msg)

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

            elif t == proto.SEARCH_FILES_REQUEST:
                self._handle_search(conn, msg)

            elif t == proto.GET_FILE_SOURCES_REQUEST:
                self._handle_get_sources(conn, msg)

            else:
                conn.sendall(proto.build(proto.ERROR, reason='unsupported_message_type'))

        except (ConnectionResetError, BrokenPipeError, socket.timeout):
            pass
        except ProtocolError as e:
            logger.warning(f"Protocolo inválido de {addr}: {e}")
            try:
                conn.sendall(proto.build(proto.ERROR, reason='protocol_error'))
            except Exception: pass
        except Exception as e:
            logger.error(f"Erro ao processar conexão de {addr}: {e}")
        finally:
            try:
                conn.close()
            except Exception: pass

    # ── handlers individuais ──────────────────────────────────

    def _handle_who_is_leader(self, conn):
        leader = self.state.current_leader
        conn.sendall(proto.build(
            proto.LEADER_INFO,
            leader=leader,
            election_in_progress=self.state.election_in_progress
        ))

    def _handle_announce(self, conn, msg):
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

        if not all([peer_name, ip_address, port]):
            conn.sendall(proto.build(proto.ERROR, reason='missing_fields'))
            return

        from storage.dict_store import register_peer, register_peer_files
        register_peer(peer_name, ip_address, port, uptime)
        register_peer_files(peer_name, files)

        self.state.add_known_peer({
            'peer_name':  peer_name,
            'ip_address': ip_address,
            'port':       port,
            'uptime':     uptime
        })

        config.add_to_bootstrap(ip_address, port)

        from storage.dict_store import get_all_peers
        peers = get_all_peers()
        conn.sendall(proto.build(proto.PEER_LIST, peers=peers))

    def _handle_file_index(self, conn, msg):
        peer_name = msg.get('peer_name')

        if peer_name and self.state.is_leader:
            if peer_name == self.state.peer_name:
                files_raw = scan_shared_folder()
                files = [
                    {'filename': f['filename'],
                     'size_bytes': f['size_bytes'],
                     'checksum': f['checksum']}
                    for f in files_raw
                ]
            else:
                from storage.dict_store import get_peer_files
                files = get_peer_files(peer_name)
        else:
            files_raw = scan_shared_folder()
            files = [
                {'filename': f['filename'],
                 'size_bytes': f['size_bytes'],
                 'checksum': f['checksum']}
                for f in files_raw
            ]

        conn.sendall(proto.build(proto.FILE_INDEX_RESP, files=files))

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
        sender_uptime = msg.get('uptime', 0)

        if self.state.uptime > sender_uptime:
            try:
                conn.sendall(proto.build(proto.ELECTION_OK))
            except Exception: pass

            if not self.state.election_in_progress:
                from discovery.election import start_election
                threading.Thread(
                    target=start_election,
                    args=(self.state,),
                    daemon=True
                ).start()
        else:
            try:
                conn.sendall(proto.build(proto.ERROR, reason='lower_uptime'))
            except Exception: pass

    def _handle_leader(self, conn, msg):
        """Recebe anúncio de novo líder e atualiza estado local."""
        self.state.current_leader = {
            'peer_name':  msg.get('peer_name'),
            'ip_address': msg.get('ip_address'),
            'port':       msg.get('port')
        }
        self.state.is_leader           = (msg.get('peer_name') == config.PEER_NAME)
        self.state.election_in_progress = False
        try:
            conn.sendall(proto.build(proto.HEARTBEAT_ACK))
        except Exception: pass
        logger.info(f"Novo líder: {msg.get('peer_name')}")

    def _handle_download(self, conn, msg):
        checksum = msg.get('checksum')
        if not checksum:
            conn.sendall(proto.build(proto.ERROR, reason='missing_checksum'))
            return

        arquivo = get_file_for_download(checksum, config.PEER_NAME)
        if not arquivo:
            conn.sendall(proto.build(proto.ERROR, reason='file_not_found'))
            return

        try:
            conn.sendall(proto.build(
                proto.FILE_DATA,
                filename=arquivo['filename'],
                size_bytes=arquivo['size_bytes'],
                checksum=arquivo['checksum']
            ))


            with open(arquivo['filepath'], 'rb') as f:
                while chunk := f.read(4096):
                    conn.sendall(chunk)
        except (BrokenPipeError, ConnectionResetError) as e:
            logger.warning(f"Cliente fechou a conexão durante o envio do arquivo '{arquivo['filename']}'.")
        except OSError as e:
            logger.error(f"Erro de disco ao enviar arquivo '{arquivo['filename']}': {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao enviar arquivo '{arquivo['filename']}': {e}")

    def _handle_search(self, conn, msg):
        if not self.state.is_leader:
            conn.sendall(proto.build(proto.ERROR, reason='not_a_leader'))
            return

        query = msg.get('query', '')
        from storage.dict_store import search_file_by_name
        results = search_file_by_name(query)
        conn.sendall(proto.build(proto.SEARCH_FILES_RESPONSE, results=results))

    def _handle_get_sources(self, conn, msg):
        if not self.state.is_leader:
            conn.sendall(proto.build(proto.ERROR, reason='not_a_leader'))
            return

        checksum = msg.get('checksum')
        if not checksum:
            conn.sendall(proto.build(proto.ERROR, reason='missing_checksum'))
            return

        from storage.dict_store import get_peers_with_file
        sources = get_peers_with_file(checksum)
        conn.sendall(proto.build(proto.GET_FILE_SOURCES_RESPONSE, sources=sources))
