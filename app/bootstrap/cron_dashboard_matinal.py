"""
Hub Comercial — cron_dashboard_matinal.py
Roda todo dia as 07:00. Envia resumo para Ailton via Telegram.
"""
import sqlite3, logging, os, sys, requests
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

DB_PATH = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_AILTON = "2135602169"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c

def notificar(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_AILTON, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        log.info("Dashboard enviado")
    except Exception as e:
        log.error(f"Telegram: {e}")

def main():
    conn = db()
    hoje = datetime.now()
    ontem = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
    data_fmt = (hoje - timedelta(days=1)).strftime("%d/%m/%Y")
    mes_inicio = hoje.strftime("%Y-%m-01")

    cad = conn.execute("""
        SELECT COUNT(*) as total,
               SUM(status='ativado') as ativados,
               SUM(status='reprovado') as reprovados,
               SUM(status='erro_ativacao') as erros,
               SUM(status='aguard_correcao') as correcao,
               SUM(status='pendente') as pendentes
        FROM hc_precadastros WHERE DATE(criado_em)=?
    """, (ontem,)).fetchone()

    ranking = conn.execute("""
        SELECT v.nome, COUNT(*) as qtd
        FROM hc_precadastros p
        JOIN hc_vendedores v ON v.id=p.ixc_vendedor_id
        WHERE DATE(p.criado_em)=? AND p.status='ativado'
        GROUP BY v.nome ORDER BY qtd DESC LIMIT 5
    """, (ontem,)).fetchall()

    erros_pend = conn.execute("SELECT COUNT(*) as t FROM hc_precadastros WHERE status='erro_ativacao'").fetchone()
    aguard = conn.execute("SELECT COUNT(*) as t FROM hc_precadastros WHERE status IN ('aguard_correcao','pendente')").fetchone()

    upgrades = conn.execute("""
        SELECT COUNT(*) as t FROM hc_upgrades_base
        WHERE status_negociacao='cliente_ciente' AND DATE(data_contato)=?
    """, (ontem,)).fetchone()

    mes = conn.execute("""
        SELECT COUNT(*) as total, SUM(status='ativado') as ativados
        FROM hc_precadastros WHERE criado_em>=?
    """, (mes_inicio,)).fetchone()

    conn.close()

    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    rank_txt = ""
    for i, r in enumerate(ranking):
        rank_txt += f"\n  {medalhas[i]} {r['nome']}: *{r['qtd']}*"
    if not rank_txt:
        rank_txt = "\n  Nenhuma ativação ontem"

    msg = f"""☀️ *Bom dia, Ailton!*
📊 *Hub Comercial — {data_fmt}*

📋 *Cadastros de ontem:*
  Total enviados: *{cad['total'] or 0}*
  ✅ Ativados: *{cad['ativados'] or 0}*
  ❌ Reprovados: *{cad['reprovados'] or 0}*
  ⚠️ Erro ativação: *{cad['erros'] or 0}*
  🔄 Aguard. correção: *{cad['correcao'] or 0}*

🏆 *Ranking de ontem:*{rank_txt}

📅 *Mês atual ({hoje.strftime('%m/%Y')}):*
  Total enviados: *{mes['total'] or 0}*
  ✅ Ativados: *{mes['ativados'] or 0}*

🔧 *Pendências acumuladas:*
  Erros de ativação: *{erros_pend['t'] or 0}*
  Aguardando correção: *{aguard['t'] or 0}*

🔄 *Upgrades cientes ontem:* *{upgrades['t'] or 0}*"""

    notificar(msg)
    log.info("Dashboard matinal enviado.")

if __name__ == "__main__":
    main()
