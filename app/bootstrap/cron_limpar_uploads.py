"""
Hub Comercial — cron_limpar_uploads.py
Roda 1x por dia (madrugada).
Deleta fotos de cadastros ativados há mais de 7 dias.
"""
import sqlite3, os, shutil, logging, requests
from pathlib import Path
from datetime import datetime

BASE_DIR    = Path(__file__).resolve().parent.parent.parent
DB_PATH     = BASE_DIR / "hub_comercial.db"
UPLOAD_DIR  = BASE_DIR / "uploads"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_AILTON = "2135602169"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")
import os
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

def notificar(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_AILTON, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")

def main():
    log.info("=== Limpeza de uploads ===")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Buscar cadastros ativados há mais de 7 dias
    rows = conn.execute("""
        SELECT id, razao FROM hc_precadastros
        WHERE status = 'ativado'
        AND atualizado_em <= datetime('now', '-7 days', '-3 hours')
    """).fetchall()
    conn.close()

    total_deletados = 0
    total_bytes = 0
    pastas_removidas = 0

    for r in rows:
        pasta = UPLOAD_DIR / str(r['id'])
        if pasta.exists():
            # Calcular tamanho
            size = sum(f.stat().st_size for f in pasta.rglob('*') if f.is_file())
            total_bytes += size
            # Deletar pasta
            shutil.rmtree(pasta)
            total_deletados += 1
            pastas_removidas += 1
            log.info(f"Deletado: {pasta} ({size/1024:.1f}KB)")

    if total_deletados > 0:
        mb = total_bytes / 1024 / 1024
        msg = (
            f"🗑️ *Limpeza de uploads — Hub Comercial*\n\n"
            f"✅ {pastas_removidas} pastas removidas\n"
            f"💾 {mb:.1f} MB liberados\n\n"
            f"_Fotos de cadastros ativados há mais de 7 dias_"
        )
        notificar(msg)
        log.info(f"Limpeza concluída: {pastas_removidas} pastas, {mb:.1f}MB liberados")
    else:
        log.info("Nenhuma pasta para limpar.")

if __name__ == "__main__":
    main()
