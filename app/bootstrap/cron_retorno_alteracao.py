"""
cron_retorno_alteracao.py
Roda de hora em hora (8h-18h seg-sab).
Verifica contratos com retorno agendado para a hora atual e notifica no Telegram.
"""
import sqlite3, os, sys, requests, logging
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

DB_PATH          = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def notificar(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        log.error(f"Telegram: {e}")

def main():
    agora = datetime.now()
    # Janela: retornos agendados para hoje
    hoje = agora.strftime("%Y-%m-%d")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    pendentes = conn.execute("""
        SELECT id, ixc_contrato_id, cliente, telefone, plano_nome,
               data_retorno, responsavel, obs
        FROM hc_alteracao_planos
        WHERE data_retorno LIKE ?
          AND retorno_enviado = 0
          AND status_alteracao != 'alterado'
          AND status_alteracao != 'recusou'
        ORDER BY data_retorno ASC
    """, (f"{hoje}%",)).fetchall()

    if not pendentes:
        log.info("Nenhum retorno pendente para hoje.")
        conn.close()
        return

    log.info(f"{len(pendentes)} retorno(s) para hoje.")

    for r in pendentes:
        msg = (
            f"⏰ *LEMBRETE DE RETORNO*\n\n"
            f"👤 *Cliente:* {r['cliente']}\n"
            f"📋 *Contrato:* #{r['ixc_contrato_id']}\n"
            f"📞 *Telefone:* {r['telefone'] or '—'}\n"
            f"📦 *Plano:* {r['plano_nome'] or '—'}\n"
            f"🕐 *Agendado para:* {r['data_retorno']}\n"
        )
        if r['obs']: msg += f"💬 *Obs:* {r['obs']}\n"
        msg += f"\n👨‍💼 *Responsável:* {r['responsavel'] or '—'}"

        notificar(msg)

        # Marca como enviado
        conn.execute(
            "UPDATE hc_alteracao_planos SET retorno_enviado=1 WHERE id=?", (r['id'],)
        )
        conn.commit()
        log.info(f"Retorno enviado: #{r['ixc_contrato_id']} {r['cliente']}")

    conn.close()

if __name__ == "__main__":
    main()
