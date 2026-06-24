"""
cron_alteracao_planos_mensal.py
Roda dia 1 de cada mês às 08:00.
Envia resumo do mês atual de contratos para alteração de plano via Telegram.
"""
import sqlite3, os, sys, requests, logging
from pathlib import Path
from datetime import datetime
import calendar

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

DB_PATH            = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MESES_PT = {
    1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
    7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"
}

def notificar(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        log.info("Notificação enviada!")
    except Exception as e:
        log.error(f"Telegram: {e}")

def main():
    from app.services.ixc_db import ixc_conn

    hoje = datetime.now()
    ano  = hoje.year
    mes  = hoje.month
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    mes_inicio = f"{ano}-{mes:02d}-01"
    mes_fim    = f"{ano}-{mes:02d}-{ultimo_dia:02d}"
    mes_label  = f"{MESES_PT[mes]}/{ano}"

    # Busca contratos do mês direto no IXC
    try:
        with ixc_conn() as ixc:
            with ixc.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        SUM(vd.valor_contrato) AS receita_risco,
                        COUNT(DISTINCT ci.nome) AS cidades
                    FROM cliente_contrato cc
                    INNER JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
                    LEFT  JOIN cidade ci        ON ci.id = (
                        SELECT cidade FROM cliente WHERE id = cc.id_cliente LIMIT 1
                    )
                    WHERE cc.status = 'A'
                      AND cc.data_expiracao >= %s
                      AND cc.data_expiracao <= %s
                """, (mes_inicio, mes_fim))
                row = cur.fetchone()

                # Busca por plano
                cur.execute("""
                    SELECT cc.contrato AS plano, COUNT(*) AS total
                    FROM cliente_contrato cc
                    WHERE cc.status = 'A'
                      AND cc.data_expiracao >= %s
                      AND cc.data_expiracao <= %s
                    GROUP BY cc.contrato
                    ORDER BY total DESC
                    LIMIT 5
                """, (mes_inicio, mes_fim))
                planos = cur.fetchall()

    except Exception as e:
        log.error(f"IXC: {e}")
        return

    total   = row["total"] or 0
    receita = float(row["receita_risco"] or 0)

    linhas_planos = "\n".join([f"  • {p['plano']}: {p['total']}" for p in planos])

    msg = (
        f"📅 *ALTERAÇÃO DE PLANOS — {mes_label}*\n\n"
        f"🗓 Contratos vencendo este mês: *{total}*\n"
        f"💰 Receita em risco: *R$ {receita:,.2f}*\n\n"
        f"📦 *Top planos do mês:*\n{linhas_planos}\n\n"
        f"👉 Acesse o painel e distribua para os atendentes!\n"
        f"🔗 https://comercial.iatechhub.com.br"
    )

    notificar(msg)
    log.info(f"Mês {mes_label}: {total} contratos, R$ {receita:,.2f}")

if __name__ == "__main__":
    main()
