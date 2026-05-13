import asyncio
import logging
import json
import uuid
import os
from datetime import datetime
from dotenv import load_dotenv
from network.protocol import Message, UserMessageType, PeerMessageType, FileMessageType, SystemMessageType
from storage import postgres
from storage import redis

# Nota: Certifique-se de que a biblioteca 'websockets' está instalada no seu ambiente.
try:
    from websockets import serve
except ImportError:
    print("Erro: A biblioteca 'websockets' não foi encontrada. Instale-a com: pip install websockets")


load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FastTrackServer")

class FastTrackServer:
    def __init__(self):
        self.handlers = {
            UserMessageType.REGISTER_REQUEST.value: self.handle_register,
            UserMessageType.LOGIN_REQUEST.value: self.handle_login,
            UserMessageType.LOGOUT_REQUEST.value: self.handle_logout,
            PeerMessageType.LIST_PEERS_REQUEST.value: self.handle_list_peers,
            FileMessageType.FILE_UPLOAD_REQUEST.value: self.handle_file_upload,
            FileMessageType.LIST_SHARED_FILES_REQUEST.value: self.handle_list_shared_files,
            FileMessageType.LIST_KNOWN_FILES_REQUEST.value: self.handle_list_known_files,
            FileMessageType.SEARCH_FILES_REQUEST.value: self.handle_search_files,
            FileMessageType.GET_FILE_SOURCES_REQUEST.value: self.handle_get_file_sources,
            SystemMessageType.HEARTBEAT.value: self.handle_heartbeat,
        }

    async def send_response(self, websocket, msg_type, payload, correlation_id=None):
        msg_type_str = msg_type.value if hasattr(msg_type, 'value') else msg_type
        response = Message(
            type=msg_type_str,
            message_id=str(uuid.uuid4()),
            correlation_id=correlation_id,
            timestamp=datetime.now().isoformat(),
            payload=payload
        )
        await websocket.send(response.serialize())

    async def send_error(self, websocket, error_msg, correlation_id=None):
        await self.send_response(websocket, SystemMessageType.ERROR, {"error": error_msg}, correlation_id)

    async def handle_message(self, websocket, data):
        try:
            # Reconstrói o objeto Message a partir do dicionário/string
            if isinstance(data, str):
                message = Message.desserialize(data)
            else:
                message = Message(
                    type=data["type"],
                    message_id=data["message_id"],
                    correlation_id=data.get("correlation_id"),
                    timestamp=data["timestamp"],
                    payload=data["payload"]
                )

            logger.info(f"Recebido {message.type} de {websocket.remote_address}")

            handler = self.handlers.get(message.type)
            if handler:
                await handler(websocket, message)
            else:
                logger.warning(f"Tipo de mensagem não suportado: {message.type}")
                await self.send_error(websocket, f"Tipo de mensagem '{message.type}' não suportado.", message.message_id)

        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
            await self.send_error(websocket, f"Erro interno ao processar requisição: {str(e)}")

    async def handler(self, websocket):
        addr = websocket.remote_address
        logger.info(f"Nova conexão estabelecida com {addr}")
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_message(websocket, data)
                except json.JSONDecodeError:
                    logger.error(f"JSON inválido recebido de {addr}")
                    await self.send_error(websocket, "Formato JSON inválido.")
        except Exception as e:
            logger.error(f"Erro na conexão com {addr}: {e}")
        finally:
            logger.info(f"Conexão finalizada com {addr}")


    async def handle_register(self, websocket, message):
        payload = message.payload
        peer_name = payload.get('peer_name')
        password = payload.get('password')
        port = payload.get('port')
        ip_address = websocket.remote_address[0]

        if not all([peer_name, password, port]):
            await self.send_error(websocket, "Campos obrigatórios ausentes (peer_name, password, port).", message.message_id)
            return

        success = postgres.insert_peer(peer_name, password, ip_address, port)
        await self.send_response(
            websocket,
            UserMessageType.REGISTER_RESPONSE,
            {"success": success, "message": "Registro concluído" if success else "Erro: Peer ou endereço já registrado."},
            message.message_id
        )

    async def handle_login(self, websocket, message):
        payload = message.payload
        peer_name = payload.get('peer_name')
        password = payload.get('password')

        success = postgres.peer_login(peer_name, password)
        await self.send_response(
            websocket,
            UserMessageType.LOGIN_RESPONSE,
            {"success": success, "message": "Login efetuado" if success else "Credenciais inválidas."},
            message.message_id
        )

    async def handle_logout(self, websocket, message):
        peer_name = message.payload.get('peer_name')
        success = postgres.peer_logout(peer_name)
        await self.send_response(websocket, UserMessageType.LOGOUT_RESPONSE, {"success": success}, message.message_id)

    async def handle_list_peers(self, websocket, message):
        peers = postgres.get_active_peers()
        await self.send_response(websocket, PeerMessageType.LIST_PEERS_RESPONSE, {"peers": peers}, message.message_id)

    async def handle_file_upload(self, websocket, message):
        payload = message.payload
        peer_name = payload.get('peer_name')
        filename = payload.get('filename')
        filepath = payload.get('filepath', '')
        size_bytes = payload.get('size_bytes')
        checksum = payload.get('checksum')

        if not all([peer_name, filename, size_bytes, checksum]):
            await self.send_error(websocket, "Metadados do arquivo incompletos.", message.message_id)
            return

        success = postgres.insert_shared_file(peer_name, filename, filepath, size_bytes, checksum)
        await self.send_response(websocket, FileMessageType.FILE_UPLOAD_RESPONSE, {"success": success}, message.message_id)

    async def handle_list_shared_files(self, websocket, message):
        peer_name = message.payload.get('peer_name')
        files = postgres.get_shared_files_by_peer(peer_name)
        await self.send_response(websocket, FileMessageType.LIST_SHARED_FILES_RESPONSE, {"files": files}, message.message_id)

    async def handle_list_known_files(self, websocket, message):
        peer_name = message.payload.get('peer_name')
        if peer_name:
            kf = redis.get_peer_files(peer_name)
            await self.send_response(websocket, FileMessageType.LIST_KNOWN_FILES_RESPONSE, {"files": kf}, message.message_id)
        else:
            await self.send_error(websocket, FileMessageType.LIST_KNOWN_FILES_RESPONSE, {"files": [], "info": "Erro ao tentar listar arquivos conhecidos."}, message.message_id)

    async def handle_search_files(self, websocket, message):
        filename = message.payload.get('query')
        if not filename:
            await self.send_error(websocket, "Query de busca vazia.", message.message_id)
            return
        
        results = redis.search_file_by_name(filename)
        await self.send_response(websocket, FileMessageType.SEARCH_FILES_RESPONSE, {"results": results}, message.message_id)

    async def handle_get_file_sources(self, websocket, message):
        checksum = message.payload.get('checksum')
        if not checksum:
            await self.send_error(websocket, "Checksum não fornecido.", message.message_id)
            return

        peer_names = redis.get_peers_with_file(checksum)
        sources = []
        for name in peer_names:
            info = redis.get_peer_info(name)
            if info:
                sources.append(info)
        
        await self.send_response(websocket, FileMessageType.GET_FILE_SOURCES_RESPONSE, {"checksum": checksum, "sources": sources}, message.message_id)

    async def handle_heartbeat(self, websocket, message):
        await self.send_response(websocket, SystemMessageType.HEARTBEAT, {"status": "alive", "server_time": datetime.now().isoformat()}, message.message_id)

async def main():
    try:
        postgres.init_postgres()
    except Exception as e:
        logger.error(f"Falha ao conectar com o banco de dados: {e}")
        return

    server_instance = FastTrackServer()
    host = os.environ.get("SERVER_HOST", "0.0.0.0")
    port = int(os.environ.get("SERVER_PORT", "8888"))

    logger.info(f"Iniciando Servidor WebSocket em {host}:{port}")
    try:
        async with serve(
            server_instance.handler,
            host,
            port,
            ping_interval=60,
            ping_timeout=120,
            close_timeout=60
        ) as server:
            await server.serve_forever()
    except Exception as e:
        logger.error(f"Erro fatal no servidor: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Servidor interrompido pelo usuário.")
