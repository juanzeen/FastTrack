import logging
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    from discovery.peer_state import PeerState
    from network.server       import PeerServer
    from discovery.bootstrap  import join_network
    from discovery.heartbeat  import HeartbeatService
    from cli.interface        import run

    state = PeerState()
    logger.info(f"Peer '{state.peer_name}' iniciando em {state.ip_address}:{state.port}")

    server = PeerServer(state)
    server.start()

    join_network(state)

    heartbeat = HeartbeatService(state)
    heartbeat.start()

    def shutdown(sig=None, frame=None):
        logger.info("Encerrando...")
        heartbeat.stop()
        server.stop()

        leader = state.current_leader
        if leader and not state.is_leader:
            from network import protocol as proto
            from network.client import send_and_receive
            send_and_receive(
                leader['ip_address'],
                leader['port'],
                proto.build(
                    proto.ANNOUNCE,
                    peer_name=state.peer_name,
                    ip_address=state.ip_address,
                    port=state.port,
                    uptime=0,
                    files=[]
                )
            )

        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── CLI ───────────────────────────────────────────────────
    try:
        run(state)
    finally:
        shutdown()


if __name__ == '__main__':
    main()
