"""
Hub Comercial — cron_sync_planos_vendedores.py
"""
import sqlite3, logging, sys
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))
from app.services.ixc_db import ixc_select

DB_PATH = BASE_DIR / "hub_comercial.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def db():
    c = sqlite3.connect(str(DB_PATH)); c.row_factory = sqlite3.Row; return c

def dec(v):
    if v is None: return None
    return float(v) if isinstance(v, Decimal) else v

def sync_planos():
    log.info("Sync planos...")
    rows = ixc_select("""
        SELECT id, nome, descricao,
               valor_contrato AS valor,
               id_tipo_documento, id_carteira_cobranca,
               id_vendedor, fidelidade, Ativo AS ativo
        FROM ixcprovedor.vd_contratos
        WHERE Ativo = 'S'
        ORDER BY id DESC
        LIMIT 10
    """)
    conn = db(); conn.execute("DELETE FROM hc_planos")
    for r in rows:
        conn.execute("""
            INSERT INTO hc_planos
                (id, nome, descricao, valor, id_tipo_documento,
                 id_carteira_cobranca, id_vendedor_padrao, fidelidade, ativo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (r["id"], r["nome"], r["descricao"], dec(r["valor"]),
              r["id_tipo_documento"], r["id_carteira_cobranca"],
              r["id_vendedor"], r["fidelidade"], r["ativo"]))
    conn.commit(); conn.close()
    log.info(f"[OK] {len(rows)} planos")

def sync_vendedores():
    log.info("Sync vendedores...")
    rows = ixc_select("""
        SELECT f.id, f.funcionario AS nome, u.nome AS login
        FROM ixcprovedor.funcionarios f
        LEFT JOIN ixcprovedor.usuarios u ON u.funcionario = f.id
        WHERE f.ativo = 'S'
        ORDER BY f.funcionario
    """)
    conn = db(); conn.execute("DELETE FROM hc_vendedores")
    for r in rows:
        conn.execute("INSERT INTO hc_vendedores(id,nome,login_ixc,ativo)VALUES(?,?,?,1)",
                     (r["id"], r["nome"], r.get("login")))
    conn.commit(); conn.close()
    log.info(f"[OK] {len(rows)} vendedores")

def sync_cidades(uf="SE"):
    log.info(f"Sync cidades UF={uf}...")
    rows = ixc_select(
        "SELECT id, nome, uf FROM ixcprovedor.cidade WHERE uf = %s ORDER BY nome", (uf,)
    )
    conn = db(); conn.execute("DELETE FROM hc_cidades_cache WHERE uf = ?", (uf,))
    for r in rows:
        conn.execute("INSERT OR REPLACE INTO hc_cidades_cache(id,nome,uf)VALUES(?,?,?)",
                     (r["id"], r["nome"], r["uf"]))
    conn.commit(); conn.close()
    log.info(f"[OK] {len(rows)} cidades")


def _salvar_log(status, resumo, duracao=0):
    try:
        import sqlite3, io
        DB = str(Path(__file__).resolve().parent.parent.parent / "hub_comercial.db")
        c = sqlite3.connect(DB, check_same_thread=False)
        c.execute("INSERT INTO hc_automacoes_log(motor,status,resumo,duracao_s) VALUES(?,?,?,?)",
                  ("Sync Planos/Vendedores", status, resumo, round(duracao,2)))
        c.commit(); c.close()
    except: pass

if __name__ == "__main__":
    try:
        sync_planos()
        sync_vendedores()
        if "--cidades" in sys.argv:
            sync_cidades("SE")
        log.info("Sync concluído.")
    except Exception as e:
        log.error(f"ERRO: {e}"); sys.exit(1)
