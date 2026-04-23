# FastTrack

## Estrutura de Pastas
```
mini-fasttrack/
│
├── peer/
│   ├── init.py
│   ├── main.py                  # Ponto de entrada do peer
│   ├── config.py                # Configurações (porta, pasta compartilhada, etc.)
│   │
│   ├── discovery/
│   │   ├── init.py
│   │   ├── node.py              # Lógica do nó de descoberta (DHT/gossip)
│   │   ├── bootstrap.py         # Lista de peers iniciais conhecidos
│   │   └── heartbeat.py        # Verificação periódica de peers ativos
│   │
│   ├── network/
│   │   ├── init.py
│   │   ├── server.py            # Servidor TCP/UDP do peer (escuta conexões)
│   │   ├── client.py            # Cliente (conecta em outros peers)
│   │   └── protocol.py          # Definição das mensagens trocadas
│   │
│   ├── files/
│   │   ├── init.py
│   │   ├── manager.py           # Gerencia pasta compartilhada, lista arquivos
│   │   ├── downloader.py        # Lógica de download direto peer-to-peer
│   │   └── integrity.py         # Verificação de integridade (hash MD5/SHA256)
│   │
│   ├── storage/
│   │   ├── init.py
│   │   └── db.py                # Conexão e queries no PostgreSQL
│   │
│   └── cli/
│       ├── init.py
│       └── interface.py         # Interface de linha de comando (terminal simples)
│
├── shared_files/                # Pasta padrão de arquivos compartilhados
│   └── .gitkeep
│
├── downloads/                   # Pasta onde chegam os downloads
│   └── .gitkeep
│
├── requirements.txt
├── .env                         # Variáveis de ambiente (porta, DB, etc.)
├── docker-compose.yml           # Sobe PostgreSQL facilmente
└── README.md
```
