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
        SELECT id, nome
        FROM ixcprovedor.vendedor
        WHERE status = 'A'
          AND id NOT IN (2, 21, 40, 29, 1)
        ORDER BY nome
    """)
    conn = db()
    # Preservar usuario_ixc_id existente antes de deletar
    usu_map = {r[0]: r[1] for r in conn.execute("SELECT id, usuario_ixc_id FROM hc_vendedores WHERE usuario_ixc_id IS NOT NULL").fetchall()}
    func_map = {r[0]: r[1] for r in conn.execute("SELECT id, funcionario_ixc_id FROM hc_vendedores WHERE funcionario_ixc_id IS NOT NULL").fetchall()}
    conn.execute("DELETE FROM hc_vendedores")
    sem_mapeamento = []
    for r in rows:
        usu_id = usu_map.get(r["id"])
        func_id = func_map.get(r["id"])
        # Alertar vendedores sem usuario_ixc_id mapeado
        if not usu_id:
            sem_mapeamento.append(f"{r['nome']} (id={r['id']})")
            log.warning(f"Vendedor sem usuario_ixc_id: {r['nome']} id={r['id']}")
        conn.execute("INSERT INTO hc_vendedores(id,nome,login_ixc,ativo,usuario_ixc_id,funcionario_ixc_id)VALUES(?,?,?,1,?,?)",
                     (r["id"], r["nome"], None, usu_id, func_id))
    conn.commit(); conn.close()
    log.info(f"[OK] {len(rows)} vendedores")
    if sem_mapeamento:
        import requests, os
        token = os.getenv("TELEGRAM_TOKEN","")
        chat = os.getenv("TELEGRAM_CHAT_ID","")
        if token and chat:
            msg = f"⚠️ *Sync Vendedores — Sem mapeamento*\n\n" + "\n".join(f"• {v}" for v in sem_mapeamento)
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": "2135602169", "text": msg, "parse_mode": "Markdown"}, timeout=10)

def sync_cidades(uf="SE"):
    log.info(f"Sync cidades UF={uf}...")
    # uf=28 é o ID numérico de Sergipe no IXC
    UF_MAP = {"SE": 28, "AL": 2, "BA": 10, "PE": 22}
    uf_id = UF_MAP.get(uf, 28)
    rows = ixc_select(
        "SELECT id, nome FROM ixcprovedor.cidade WHERE uf = %s ORDER BY nome", (uf_id,)
    )
    conn = db(); conn.execute("DELETE FROM hc_cidades_cache WHERE uf = ?", (uf,))
    for r in rows:
        conn.execute("INSERT OR REPLACE INTO hc_cidades_cache(id,nome,uf)VALUES(?,?,?)",
                     (r["id"], r["nome"], uf))
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
        sync_cidades("SE")
        log.info("Sync concluído.")
    except Exception as e:
        log.error(f"ERRO: {e}")
        try:
            import requests as _req
            token = os.getenv("TELEGRAM_TOKEN")
            if token:
                _req.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": "2135602169",
                                "text": f"🚨 *ERRO CRITICO — Sync Hub Comercial*

`{str(e)[:300]}`",
                                "parse_mode": "Markdown"}, timeout=10)
        except: pass
        sys.exit(1)
