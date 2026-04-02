"""
Hub Comercial — app/services/ixc_db.py
Conexão MySQL com o IXC Soft.
"""
import pymysql, pymysql.cursors, os, logging
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
log = logging.getLogger(__name__)

def _cfg():
    return dict(host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT",3306)),
                user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME","ixcprovedor"), charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor, connect_timeout=10)

@contextmanager
def ixc_conn():
    conn = None
    try:
        conn = pymysql.connect(**_cfg())
        cur = conn.cursor()
        cur.execute("SET NAMES utf8mb4")
        cur.execute("SET SESSION time_zone = '-03:00'")
        yield conn
    except pymysql.Error as e:
        log.error(f"[IXC DB] {e}"); raise
    finally:
        if conn: conn.close()

def ixc_select(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); return cur.fetchall()

def ixc_select_one(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); return cur.fetchone()

def ixc_insert(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); c.commit(); return cur.lastrowid

def testar_conexao():
    try: return ixc_select_one("SELECT 1 AS ok") is not None
    except: return False
