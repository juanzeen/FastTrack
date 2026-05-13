#Arquivo que define o protocolo de comunicação entre o cliente e o servidor, estruturando as mensagens e os tipos de dados que serão trocados.
from dataclasses import dataclass, asdict
from enum import Enum
import json

class UserMessageType(Enum):
    REGISTER_REQUEST = "REGISTER_REQUEST"
    REGISTER_RESPONSE = "REGISTER_RESPONSE"
    LOGIN_REQUEST = "LOGIN_REQUEST"
    LOGIN_RESPONSE = "LOGIN_RESPONSE"
    LOGOUT_REQUEST = "LOGOUT_REQUEST"
    LOGOUT_RESPONSE = "LOGOUT_RESPONSE"

class PeerMessageType(Enum):
    LIST_PEERS_REQUEST = "LIST_PEERS_REQUEST"
    LIST_PEERS_RESPONSE = "LIST_PEERS_RESPONSE"

class FileMessageType(Enum):
    FILE_UPLOAD_REQUEST = "FILE_UPLOAD_REQUEST"
    FILE_UPLOAD_RESPONSE = "FILE_UPLOAD_RESPONSE"
    FILE_DOWNLOAD_REQUEST = "FILE_DOWNLOAD_REQUEST"
    FILE_DOWNLOAD_RESPONSE = "FILE_DOWNLOAD_RESPONSE"
    LIST_SHARED_FILES_REQUEST = "LIST_SHARED_FILES_REQUEST"
    LIST_SHARED_FILES_RESPONSE = "LIST_SHARED_FILES_RESPONSE"
    LIST_KNOWN_FILES_REQUEST = "LIST_KNOWN_FILES_REQUEST"
    LIST_KNOWN_FILES_RESPONSE = "LIST_KNOWN_FILES_RESPONSE"
    SEARCH_FILES_REQUEST = "SEARCH_FILES_REQUEST"
    SEARCH_FILES_RESPONSE = "SEARCH_FILES_RESPONSE"
    GET_FILE_SOURCES_REQUEST = "GET_FILE_SOURCES_REQUEST"
    GET_FILE_SOURCES_RESPONSE = "GET_FILE_SOURCES_RESPONSE"

class SystemMessageType(Enum):
    HEARTBEAT = "HEARTBEAT"
    ERROR = "ERROR"

ALLOWED_MESSAGE_TYPES = UserMessageType | PeerMessageType | FileMessageType | SystemMessageType

@dataclass
class Message:
    type: str
    message_id: str
    #O correlation_id deve ser usado somente para respostas, apontando qual foi a request
    correlation_id: str = None
    timestamp: str
    payload: dict

    #Verifica se o tipo da mensagem é permitido, se não for, lança erro.
    def __post_init__(self):
        if self.type not in ALLOWED_MESSAGE_TYPES:
            raise ValueError(f"Mensagem do tipo {self.type} não é válida.")

    def serialize(self) -> str:
        dct = asdict(self)
        return json.dumps(dct)

    @staticmethod
    def desserialize(json_str: str) -> 'Message':
        data = json.loads(json_str)
        return Message(
            type=data["type"],
            message_id=data["message_id"],
            correlation_id=data.get("correlation_id"),
            timestamp=data["timestamp"],  # Timestamp pode ser gerado no momento da criação da mensagem
            payload=data["payload"]
        )
