"""
Monitora dois cenários de divergência:
1. Contrato ativo SEM OS de instalação finalizada (OS aberta ou sem OS)
2. Contrato ativado ANTES do fechamento da OS (OS já fechada mas foi depois da ativação)
"""
import os, pymysql, pymysql.cursors, requests, logging, sqlite3
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

def notificar(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")

def main():
    log.info("=== Monitor divergencias ativacao x OS ===")
    c = pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        database="ixcprovedor", cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4")
    cur = c.cursor()

    # CASO 1: contrato ATIVO sem OS finalizada (OS ainda aberta ou sem OS)
    cur.execute("""
        SELECT cc.id, cl.razao, cc.data_ativacao,
               v.nome as vendedor,
               TIMESTAMPDIFF(HOUR, cc.data_ativacao, NOW()) as horas,
               CASE WHEN o_aberta.id IS NOT NULL THEN 'OS aberta' ELSE 'Sem OS' END as situacao
        FROM cliente_contrato cc
        JOIN cliente cl ON cl.id = cc.id_cliente
        LEFT JOIN vendedor v ON v.id = cc.id_vendedor_ativ
        LEFT JOIN su_oss_chamado o_final ON o_final.id_contrato_kit = cc.id
            AND o_final.id_assunto IN (227,75,92,15) AND o_final.status = 'F'
        LEFT JOIN su_oss_chamado o_aberta ON o_aberta.id_contrato_kit = cc.id
            AND o_aberta.id_assunto IN (227,75,92,15) AND o_aberta.status != 'F'
        WHERE cc.status_internet = 'A'
          AND cc.data_ativacao >= DATE_FORMAT(NOW(), '%Y-%m-01')
          AND o_final.id IS NULL
        ORDER BY cc.data_ativacao DESC
    """)
    caso1 = cur.fetchall()

    # CASO 2: contrato ativo onde OS foi finalizada DEPOIS da ativação (diff > 0 horas)
    cur.execute("""
        SELECT cc.id, cl.razao, cc.data_ativacao,
               v.nome as vendedor,
               o.data_fechamento,
               'OS fechada depois' as situacao
        FROM cliente_contrato cc
        JOIN cliente cl ON cl.id = cc.id_cliente
        LEFT JOIN vendedor v ON v.id = cc.id_vendedor_ativ
        JOIN su_oss_chamado o ON o.id_contrato_kit = cc.id
            AND o.id_assunto IN (227,75,92,15) AND o.status = 'F'
        WHERE cc.status_internet = 'A'
          AND cc.data_ativacao >= DATE_FORMAT(NOW(), '%Y-%m-01')
          AND DATE(o.data_fechamento) > cc.data_ativacao
        ORDER BY o.data_fechamento DESC
    """)
    caso2 = cur.fetchall()
    c.close()

    todos = list(caso1) + list(caso2)
    if not todos:
        log.info("Nenhuma divergencia encontrada.")
        return

    # Filtrar apenas contratos ainda não notificados
    conn = sqlite3.connect(str(BASE_DIR / "hub_comercial.db"))
    conn.row_factory = sqlite3.Row
    ja_notificados = {r["ixc_contrato_id"] for r in conn.execute("SELECT ixc_contrato_id FROM hc_monitor_sem_os_log").fetchall()}

    caso1 = [r for r in caso1 if r["id"] not in ja_notificados]
    caso2 = [r for r in caso2 if r["id"] not in ja_notificados]
    todos = list(caso1) + list(caso2)

    if not todos:
        log.info("Sem novas divergencias para notificar.")
        conn.close()
        return

    log.info(f"Caso1={len(caso1)} sem OS | Caso2={len(caso2)} OS fechada depois")

    linhas = [
        "⚠️ *CLIQUEDF — Ativação antes da instalação*",
        f"_Divergências no processo de ativação_",
        ""
    ]

    if caso1:
        linhas.append(f"🔴 *Ativos SEM OS finalizada: {len(caso1)}*")
        for r in caso1[:10]:
            vend = (r['vendedor'] or '?').split()[0].title()
            dias = r['horas'] // 24
            horas = r['horas'] % 24
            tempo = f"{dias}d {horas}h" if dias > 0 else f"{horas}h"
            linhas.append(f"  • #{r['id']} {r['razao'][:24]} — {vend} — {tempo} — {r['situacao']}")
        if len(caso1) > 10:
            linhas.append(f"  _...e mais {len(caso1)-10}_")
        linhas.append("")

    if caso2:
        linhas.append(f"🟡 *Ativados ANTES do fechamento da OS: {len(caso2)}*")
        for r in caso2[:10]:
            vend = (r['vendedor'] or '?').split()[0].title()
            fechada = str(r['data_fechamento'])[:10]
            linhas.append(f"  • #{r['id']} {r['razao'][:24]} — {vend} — ativado {r['data_ativacao']} | OS fechada {fechada}")
        if len(caso2) > 10:
            linhas.append(f"  _...e mais {len(caso2)-10}_")
        linhas.append("")

    linhas.append(f"📊 Total: *{len(todos)} divergências*")
    notificar("\n".join(linhas))
    log.info("Alerta enviado.")

    # Salvar contratos notificados para não repetir
    from datetime import datetime
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for r in caso1:
        conn.execute("INSERT OR IGNORE INTO hc_monitor_sem_os_log(ixc_contrato_id, razao, data_ativacao, data_os_fechamento, notificado_em) VALUES(?,?,?,?,?)",
            (r["id"], r["razao"], str(r["data_ativacao"]), None, agora))
    for r in caso2:
        conn.execute("INSERT OR IGNORE INTO hc_monitor_sem_os_log(ixc_contrato_id, razao, data_ativacao, data_os_fechamento, notificado_em) VALUES(?,?,?,?,?)",
            (r["id"], r["razao"], str(r["data_ativacao"]), str(r["data_fechamento"]), agora))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
