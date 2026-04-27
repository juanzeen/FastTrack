import psycopg2
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

conn = psycopg2.connect(
    host='localhost',
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
    )

cur = conn.cursor()

cur.execute('''
            CREATE TABLE IF NOT EXISTS peers (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                peer_name VARCHAR(255) NOT NULL UNIQUE,
                ip_address VARCHAR(45) NOT NULL,
                port INTEGER NOT NULL,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'inactive' CHECK (status IN ('active', 'inactive')),
                UNIQUE(ip_address, port)
            );
''')

cur.execute('''
            CREATE TABLE IF NOT EXISTS shared_files (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                peer_id UUID NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                filename VARCHAR(255) NOT NULL,
                filepath VARCHAR(255) NOT NULL,
                size_bytes BIGINT NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
''')

cur.execute('''
            CREATE TABLE IF NOT EXISTS downloads (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                filename VARCHAR(255) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                source_peer_name VARCHAR(255) NOT NULL,
                source_peer_ip VARCHAR(45) NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'downloading' CHECK (status IN ('downloading', 'completed', 'failed', 'canceled'))
            );
''')

conn.commit()
cur.close()
conn.close()
