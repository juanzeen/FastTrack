import logging
import os
import config
from network import client
from files.manager import scan_shared_folder

logger = logging.getLogger(__name__)


def _get_my_ip() -> str:
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def run(state):
    print(f"\n{'='*50}")
    print(f"  Mini FastTrack — {state.peer_name}")
    print(f"  IP: {state.ip_address}  Porta: {state.port}")
    print(f"{'='*50}")
    print("  Digite 'help' para ver os comandos.")
    print()

    while True:
        try:
            raw = input("fasttrack> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSaindo...")
            break

        if not raw:
            continue

        parts   = raw.split()
        command = parts[0].lower()
        args    = parts[1:]

        if command == 'help':
            _cmd_help()

        elif command == 'peers':
            _cmd_peers(state)

        elif command == 'files':
            _cmd_files(state, args)

        elif command == 'search':
            _cmd_search(state, args)

        elif command == 'download':
            _cmd_download(state, args)

        elif command == 'myfiles':
            _cmd_myfiles()

        elif command == 'status':
            _cmd_status(state)

        elif command == 'exit' or command == 'quit':
            print("Saindo da rede...")
            break

        else:
            print(f"Comando desconhecido: '{command}'. Digite 'help'.")


def _cmd_help():
    print()
    print("  peers                    → lista peers ativos na rede")
    print("  files [peer_name]        → lista arquivos de um peer (ou do super nó)")
    print("  search <nome>            → busca arquivo por nome na rede")
    print("  download <checksum>      → baixa arquivo pelo checksum")
    print("  myfiles                  → lista seus arquivos compartilhados")
    print("  status                   → mostra estado atual do peer")
    print("  exit                     → sai da rede")
    print()


def _cmd_peers(state):
    leader = state.current_leader
    if not leader:
        print("Sem líder conhecido no momento.")
        return

    from network import protocol as proto
    resp = client.send_and_receive(
        leader['ip_address'],
        leader['port'],
        proto.build(proto.GET_PEERS)
    )

    peers = []
    if resp and resp.get('type') == 'PEER_LIST':
        peers = resp.get('peers', [])
    else:
        peers = state.known_peers

    if not peers and leader:
        peers = [leader]

    if not peers:
        print("Nenhum peer ativo encontrado.")
        return

    print(f"\n  {'NOME':<20} {'IP':<16} {'PORTA':<8} {'UPTIME'}")
    print(f"  {'-'*60}")
    for p in peers:
        uptime = p.get('uptime', 0)
        horas  = int(uptime // 3600)
        mins   = int((uptime % 3600) // 60)
        segs   = int(uptime % 60)
        print(f"  {p['peer_name']:<20} {p['ip_address']:<16} {p['port']:<8} {horas:02d}:{mins:02d}:{segs:02d}")
    print()


def _cmd_files(state, args):
    leader = state.current_leader
    if not leader:
        print("Sem líder conhecido no momento.")
        return

    from network import protocol as proto
    if args:
        peer_name = args[0]
        resp = client.send_and_receive(
            leader['ip_address'],
            leader['port'],
            proto.build('FILE_INDEX', peer_name=peer_name)
        )
        files = resp.get('files', []) if resp else []
    else:
        from storage.dict_store import search_file_by_name
        if state.is_leader:
            files = search_file_by_name('')
        else:
            print("Use 'search <nome>' para buscar arquivos na rede.")
            return

    if not files:
        print("Nenhum arquivo encontrado.")
        return

    _print_files(files)


def _cmd_search(state, args):
    if not args:
        print("Uso: search <nome_do_arquivo>")
        return

    query  = ' '.join(args)
    leader = state.current_leader

    if not leader:
        print("Sem líder conhecido no momento.")
        return

    if state.is_leader:
        from storage.dict_store import search_file_by_name
        results = search_file_by_name(query)
    else:
        from network import protocol as proto
        resp = client.send_and_receive(
            leader['ip_address'],
            leader['port'],
            proto.build('SEARCH_FILES_REQUEST', query=query)
        )
        results = resp.get('results', []) if resp else []

    if not results:
        print(f"Nenhum arquivo encontrado para '{query}'.")
        return

    print(f"\n  Resultados para '{query}':")
    print(f"  {'NOME':<30} {'TAMANHO':<12} {'CHECKSUM':<20} PEERS")
    print(f"  {'-'*80}")
    for r in results:
        size  = _format_size(r.get('size_bytes', 0))
        chk   = r['checksum'][:16] + '...'
        peers = ', '.join(p['peer_name'] for p in r.get('peers', []))
        print(f"  {r['filename']:<30} {size:<12} {chk:<20} {peers}")
    print()
    print("  Use: download <checksum_completo>")
    print()


def _cmd_download(state, args):
    if not args:
        print("Uso: download <checksum>")
        return

    checksum = args[0]
    leader   = state.current_leader

    if not leader:
        print("Sem líder conhecido.")
        return

    if state.is_leader:
        from storage.dict_store import get_peers_with_file
        sources = get_peers_with_file(checksum)
    else:
        from network import protocol as proto
        resp = client.send_and_receive(
            leader['ip_address'],
            leader['port'],
            proto.build('GET_FILE_SOURCES_REQUEST', checksum=checksum)
        )
        sources = resp.get('sources', []) if resp else []

    if not sources:
        print(f"Nenhum peer tem o arquivo com checksum '{checksum[:16]}...'")
        return

    for source in sources:
        if source['peer_name'] == state.peer_name:
            continue

        print(f"Baixando de '{source['peer_name']}'...")
        status = client.download_file(
            source['peer_name'],
            source['ip_address'],
            source['port'],
            checksum,
            f"download_{checksum[:8]}"
        )

        if status == 'completed':
            print(f"Download concluído. Arquivo salvo em '{config.DOWNLOAD_FOLDER}'.")
            return
        elif status == 'not_found':
            print(f"Peer '{source['peer_name']}' não tem mais o arquivo. Tentando próximo...")
        else:
            print(f"Falha com '{source['peer_name']}'. Tentando próximo...")

    print("Não foi possível baixar o arquivo de nenhum peer disponível.")


def _cmd_myfiles():
    arquivos = scan_shared_folder()
    if not arquivos:
        print(f"Nenhum arquivo em '{config.SHARED_FOLDER}'.")
        return

    print(f"\n  {'NOME':<30} {'TAMANHO':<12} CHECKSUM")
    print(f"  {'-'*70}")
    for f in arquivos:
        size = _format_size(f['size_bytes'])
        print(f"  {f['filename']:<30} {size:<12} {f['checksum']}")
    print()


def _cmd_status(state):
    leader = state.current_leader
    uptime = state.uptime
    horas  = int(uptime // 3600)
    mins   = int((uptime % 3600) // 60)
    segs   = int(uptime % 60)

    print()
    print(f"  Peer:     {state.peer_name}")
    print(f"  IP:       {state.ip_address}:{state.port}")
    print(f"  Uptime:   {horas:02d}:{mins:02d}:{segs:02d}")
    print(f"  Líder:    {'EU MESMO' if state.is_leader else (leader['peer_name'] if leader else 'desconhecido')}")
    print(f"  Eleição:  {'em andamento' if state.election_in_progress else 'nenhuma'}")
    print(f"  Peers conhecidos: {len(state.known_peers)}")
    print()


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes/1024:.1f}KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes/1024**2:.1f}MB"
    return f"{size_bytes/1024**3:.1f}GB"


def _print_files(files: list):
    print(f"\n  {'NOME':<30} {'TAMANHO':<12} CHECKSUM")
    print(f"  {'-'*70}")
    for f in files:
        size = _format_size(f.get('size_bytes', 0))
        print(f"  {f['filename']:<30} {size:<12} {f['checksum']}")
    print()
