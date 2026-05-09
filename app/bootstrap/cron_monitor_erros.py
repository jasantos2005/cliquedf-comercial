"""
Hub Comercial — cron_monitor_erros.py
======================================
Roda a cada 1 hora via crontab.
Detecta erros do sistema, corrige o que for possivel
e notifica Ailton via Telegram pessoal.
"""
import sqlite3, logging, os, sys, requests, unicodedata
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")
import pymysql, pymysql.cursors

DB_PATH = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_AILTON = "2135602169"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def ixc():
    return pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        database="ixcprovedor", cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )


def notificar(msg):
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN nao configurado")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_AILTON, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        log.info("Telegram enviado")
    except Exception as e:
        log.error(f"Telegram erro: {e}")


def verificar_servico():
    import subprocess
    r = subprocess.run(["systemctl", "is-active", "hubcomercial_cliquedf"],
                       capture_output=True, text=True)
    if r.stdout.strip() != "active":
        log.warning("Servico INATIVO — reiniciando...")
        subprocess.run(["systemctl", "restart", "hubcomercial_cliquedf"])
        return "Servico estava inativo — *reiniciado automaticamente* ✅"
    return None


def corrigir_cidade():
    conn = db()
    erros = conn.execute("""
        SELECT DISTINCT p.id, p.razao, p.cidade_nome, p.ixc_cidade_id
        FROM hc_precadastros p
        JOIN hc_ativacoes_log al ON al.precadastro_id = p.id
        WHERE p.status = 'erro_ativacao'
        AND al.sucesso = 0
        AND (al.erro_msg LIKE '%cidade%' OR al.erro_msg LIKE '%cliente_ibfk_3%')
        ORDER BY p.id DESC LIMIT 50
    """).fetchall()

    if not erros:
        conn.close()
        return []

    cx = ixc()
    cur = cx.cursor()
    corrigidos = []

    for e in erros:
        nome = e["cidade_nome"] or ""
        if not nome:
            continue
        cur.execute("SELECT id FROM cidade WHERE nome LIKE %s AND uf=28 LIMIT 1", (f"%{nome}%",))
        cidade = cur.fetchone()
        if not cidade:
            sem_acento = ''.join(c for c in unicodedata.normalize('NFD', nome)
                                 if unicodedata.category(c) != 'Mn')
            cur.execute("SELECT id FROM cidade WHERE nome LIKE %s AND uf=28 LIMIT 1", (f"%{sem_acento}%",))
            cidade = cur.fetchone()
        if cidade:
            conn.execute("""
                UPDATE hc_precadastros
                SET ixc_cidade_id=?, status='aprovado', atualizado_em=datetime('now','-3 hours')
                WHERE id=?
            """, (cidade["id"], e["id"]))
            corrigidos.append(f"#{e['id']} {e['razao']} ({nome})")
            log.info(f"Cidade corrigida: {e['razao']} → id={cidade['id']}")
        else:
            log.warning(f"Cidade nao encontrada no IXC: {nome}")

    conn.commit()
    conn.close()
    cx.close()
    return corrigidos


def corrigir_sem_vendedor():
    conn = db()
    rows = conn.execute("""
        SELECT id, razao, id_vendedor_hub FROM hc_precadastros
        WHERE status = 'ativado' AND ixc_vendedor_id IS NULL
        AND criado_em >= datetime('now', '-24 hours', '-3 hours')
    """).fetchall()
    corrigidos = []
    for p in rows:
        usu = conn.execute(
            "SELECT ixc_funcionario_id FROM hc_usuarios WHERE id=?",
            (p["id_vendedor_hub"],)
        ).fetchone()
        if usu and usu["ixc_funcionario_id"]:
            conn.execute("UPDATE hc_precadastros SET ixc_vendedor_id=? WHERE id=?",
                         (usu["ixc_funcionario_id"], p["id"]))
            corrigidos.append(f"#{p['id']} {p['razao']}")
            log.info(f"Vendedor corrigido: {p['razao']}")
    conn.commit()
    conn.close()
    return corrigidos


def erros_manuais():
    conn = db()
    rows = conn.execute("""
        SELECT p.id, p.razao, al.erro_msg
        FROM hc_precadastros p
        JOIN hc_ativacoes_log al ON al.precadastro_id = p.id
        WHERE p.status = 'erro_ativacao' AND al.sucesso = 0
        AND al.erro_msg NOT LIKE '%cidade%'
        AND al.erro_msg NOT LIKE '%cliente_ibfk_3%'
        AND p.criado_em >= datetime('now', '-24 hours', '-3 hours')
        GROUP BY p.id ORDER BY p.id DESC LIMIT 10
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    log.info("=== Monitor Hub Comercial ===")
    linhas = []
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")

    # 1. Servico
    msg_serv = verificar_servico()
    if msg_serv:
        linhas.append(f"🚨 {msg_serv}")

    # 2. Corrigir cidades
    cidades = corrigir_cidade()
    if cidades:
        linhas.append(f"*Cidade corrigida automaticamente ({len(cidades)}):*")
        for c in cidades:
            linhas.append(f"  ✅ {c}")

    # 3. Corrigir sem vendedor
    vendedores = corrigir_sem_vendedor()
    if vendedores:
        linhas.append(f"*Vendedor corrigido automaticamente ({len(vendedores)}):*")
        for v in vendedores:
            linhas.append(f"  ✅ {v}")

    # 4. Erros manuais
    manuais = erros_manuais()
    if manuais:
        linhas.append(f"*⚠️ Erros que precisam atenção manual ({len(manuais)}):*")
        for e in manuais[:5]:
            msg = (e["erro_msg"] or "")[:80]
            linhas.append(f"  ❌ #{e['id']} {e['razao']}: {msg}")

    if not linhas:
        log.info("Nenhum erro encontrado.")
        return

    msg = f"🤖 *Hub Comercial Monitor* `{agora}`\n\n" + "\n".join(linhas)
    notificar(msg)
    log.info(f"Notificacao enviada. {len(linhas)} itens.")


if __name__ == "__main__":
    main()
