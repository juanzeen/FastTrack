import psycopg2
import psycopg2.extras
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(
        host='localhost',
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
        )

def init_postgres():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute('''
                    CREATE TABLE IF NOT EXISTS peers (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        peer_name VARCHAR(255) NOT NULL UNIQUE,
                        password VARCHAR(255) NOT NULL,
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (peer_id, checksum)
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
        logger.info("PostgreSQL inicializado com sucesso")

    except Exception as e:
        logger.error(f"Erro ao inicializar o PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()
    
def insert_peer(peer_name, password, ip_address, port):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            INSERT INTO peers (peer_name, password, ip_address, port, status, last_seen)
            VALUES (%s, %s, %s, %s, 'active', CURRENT_TIMESTAMP);
        ''', (peer_name, password, ip_address, port))
 
        conn.commit()
        return True
    except psycopg2.errors.UniqueViolation:
        logger.warning(f"Peer name '{peer_name}' ou endereço '{ip_address}:{port}' já registrado na rede.")
        return False
    except Exception as e:
        logger.error(f"Erro ao inserir peer '{peer_name}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def peer_login(peer_name, password):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            UPDATE peers
            SET status = 'active', last_seen = CURRENT_TIMESTAMP
            WHERE peer_name = %s AND password = %s;
        ''', (peer_name, password))
 
        if cur.rowcount == 0:
            logger.warning(f"Nome do peer ou senha incorretos")
            return False
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao fazer login do peer '{peer_name}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def peer_logout(peer_name):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            UPDATE peers
            SET status = 'inactive', last_seen = CURRENT_TIMESTAMP
            WHERE peer_name = %s;
        ''', (peer_name,))
 
        if cur.rowcount == 0:
            logger.warning(f"Peer '{peer_name}' não encontrado para logout.")
            return False
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao fazer logout do peer '{peer_name}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def get_peer_by_name(peer_name):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT id, peer_name, ip_address, port, last_seen, status
            FROM peers
            WHERE peer_name = %s;
        ''', (peer_name,))

        row = cur.fetchone()

        if not row:
            logger.warning(f"Peer '{peer_name}' não encontrado")
            return None
        
        return dict(row) if row else None
 
    except Exception as e:
        logger.error(f"Erro ao buscar peer '{peer_name}': {e}")
        return None
    finally:
        cur.close()
        conn.close()

def get_all_peers():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT id, peer_name, ip_address, port, last_seen, status
            FROM peers
            ORDER BY last_seen DESC;
        ''')

        rows = cur.fetchall()

        if not rows:
            logger.warning(f"Nenhum peer foi encontrado")
            return []
 
        return [dict(row) for row in rows]
 
    except Exception as e:
        logger.error(f"Erro ao listar peers: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_active_peers():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT id, peer_name, ip_address, port, last_seen
            FROM peers
            WHERE status = 'active'
            ORDER BY last_seen DESC;
        ''')

        rows = cur.fetchall()

        if not rows:
            logger.warning(f"Nenhum peer ativo foi encontrado")
            return []
 
        return [dict(row) for row in rows]
 
    except Exception as e:
        logger.error(f"Erro ao listar peers ativos: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def insert_shared_file(peer_name, filename, filepath,
                        size_bytes, checksum):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM peers WHERE peer_name = %s AND status = 'active';", (peer_name,))
        row = cur.fetchone()
 
        if not row:
            logger.error(f"Peer '{peer_name}' não está ativo ou não foi encontrado para compartilhar um arquivo.")
            return False
 
        peer_id = row[0]

        cur.execute('''
            SELECT filename FROM shared_files
            WHERE peer_id = %s AND checksum = %s;
        ''', (peer_id, checksum))
        
        existing = cur.fetchone()
        if existing:
            logger.warning(f"Arquivo '{filename}' já está sendo compartilhado.")
            return False
 
        cur.execute('''
            INSERT INTO shared_files (peer_id, filename, filepath, size_bytes, checksum)
            VALUES (%s, %s, %s, %s, %s);
        ''', (peer_id, filename, filepath, size_bytes, checksum))
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao compartilhar arquivo '{filename}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def get_shared_files_by_peer(peer_name):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT sf.id, sf.filename, sf.filepath, sf.size_bytes, sf.checksum, sf.created_at
            FROM shared_files sf
            JOIN peers p ON sf.peer_id = p.id
            WHERE p.peer_name = %s;
        ''', (peer_name,))
 
        return [dict(row) for row in cur.fetchall()]
 
    except Exception as e:
        logger.error(f"Erro ao buscar arquivos do peer '{peer_name}': {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_shared_files_by_checksum(checksum):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT sf.id, p.peer_name, sf.filename, sf.filepath, sf.size_bytes, sf.checksum
            FROM shared_files sf
            JOIN peers p ON sf.peer_id = p.id
            WHERE checksum = %s;
        ''', (checksum,))
 
        return [dict(row) for row in cur.fetchall()]
 
    except Exception as e:
        logger.error(f"Erro ao buscar arquivo por checksum '{checksum}': {e}")
        return []
    finally:
        cur.close()
        conn.close()

def delete_shared_file(checksum, peer_name):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            DELETE FROM shared_files
            WHERE checksum = %s AND peer_id = (SELECT id FROM peers WHERE peer_name = %s);
        ''', (checksum, peer_name))
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao deletar arquivo '{checksum}' do peer '{peer_name}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def delete_all_shared_files_by_peer(peer_name):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            DELETE FROM shared_files
            WHERE peer_id = (SELECT id FROM peers WHERE peer_name = %s);
        ''', (peer_name,))
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao limpar arquivos do peer '{peer_name}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def insert_download(filename, checksum,
                     source_peer_name, source_peer_ip):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        cur.execute('''
            INSERT INTO downloads (filename, checksum, source_peer_name, source_peer_ip, status)
            VALUES (%s, %s, %s, %s, 'downloading')
            RETURNING id;
        ''', (filename, checksum, source_peer_name, source_peer_ip))
 
        download_id = cur.fetchone()[0]
        conn.commit()
        return str(download_id)
 
    except Exception as e:
        logger.error(f"Erro ao registrar download de '{filename}': {e}")
        return None
    finally:
        cur.close()
        conn.close()

def update_download_status(download_id, status):
    try:
        conn = get_connection()
        cur = conn.cursor()
 
        if status in ('completed', 'failed', 'canceled'):
            cur.execute('''
                UPDATE downloads
                SET status = %s, finished_at = CURRENT_TIMESTAMP
                WHERE id = %s;
            ''', (status, download_id))
        else:
            logger.warning(f"Status '{status}' de download não existente")
            return False
 
        conn.commit()
        return True
 
    except Exception as e:
        logger.error(f"Erro ao atualizar download '{download_id}': {e}")
        return False
    finally:
        cur.close()
        conn.close()

def get_download_history():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
 
        cur.execute('''
            SELECT id, filename, checksum, source_peer_name,
                   source_peer_ip, started_at, finished_at, status
            FROM downloads
            ORDER BY started_at DESC;
        ''')
 
        return [dict(row) for row in cur.fetchall()]
 
    except Exception as e:
        logger.error(f"Erro ao buscar histórico de downloads: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_downloads_by_status(status):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if status not in ('downloading', 'completed', 'failed', 'canceled'):
            logger.warning(f"Status '{status}' de download não existente")
            return []

        cur.execute('''
            SELECT id, filename, checksum, source_peer_name,
                   source_peer_ip, started_at, finished_at, status
            FROM downloads
            WHERE status = %s
            ORDER BY started_at DESC;
        ''', (status,))
 
        return [dict(row) for row in cur.fetchall()]
 
    except Exception as e:
        logger.error(f"Erro ao buscar downloads com status '{status}': {e}")
        return []
    finally:
        cur.close()
        conn.close()