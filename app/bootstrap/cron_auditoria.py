"""Hub Comercial — cron_auditoria.py"""
import sqlite3, logging, sys, os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))
from app.engines.auditoria_engine import auditar

DB_PATH = BASE_DIR / "hub_comercial.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False); c.row_factory = sqlite3.Row; return c

def enviar_telegram(msg):
    token = os.getenv("TELEGRAM_TOKEN"); chat = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat: return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id":chat,"text":msg,"parse_mode":"Markdown"}, timeout=10)
    except Exception as e: log.warning(f"Telegram: {e}")

def processar():
    conn = get_db(); cur = conn.cursor()
    pendentes = cur.execute("""
        SELECT p.*, v.nome AS vendedor_nome FROM hc_precadastros p
        LEFT JOIN hc_usuarios v ON v.id=p.id_vendedor_hub
        WHERE p.status='enviado' ORDER BY p.criado_em ASC LIMIT 20
    """).fetchall()
    if not pendentes: log.info("Nenhum pendente."); conn.close(); return
    log.info(f"Processando {len(pendentes)} cadastro(s)...")
    for row in pendentes:
        p = dict(row); pid = p["id"]
        docs = [dict(d) for d in cur.execute(
            "SELECT tipo,arquivo FROM hc_precadastro_docs WHERE precadastro_id=?",(pid,)).fetchall()]
        cur.execute("UPDATE hc_precadastros SET status='em_auditoria',atualizado_em=datetime('now','-3 hours') WHERE id=?",(pid,)); conn.commit()
        try: resultado = auditar(p, docs)
        except Exception as e:
            log.error(f"#{pid}: {e}")
            cur.execute("UPDATE hc_precadastros SET status='pendente',atualizado_em=datetime('now','-3 hours') WHERE id=?",(pid,)); conn.commit(); continue
        rodada = cur.execute("SELECT COALESCE(MAX(rodada),0)+1 FROM hc_auditoria_log WHERE precadastro_id=?",(pid,)).fetchone()[0]
        for r in resultado["regras"]:
            if r["resultado"]=="ok": continue
            cur.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,?,?,?,?,?)",
                (pid,rodada,r["regra"],r["legenda"],r["resultado"],r["detalhe"]))
        status_map={"aprovado":"aprovado","aprovado_com_ressalva":"aprovado","pendente":"pendente","reprovado":"reprovado"}
        novo = status_map.get(resultado["resultado_final"],"pendente")
        cur.execute("UPDATE hc_precadastros SET status=?,atualizado_em=datetime('now','-3 hours') WHERE id=?",(novo,pid)); conn.commit()
        log.info(f"#{pid} {p.get('razao','?')[:30]} → {novo}")
        vendedor=(p.get("vendedor_nome") or "Vendedor").upper()
        cliente=(p.get("razao") or "—").upper()
        proto=p.get("protocolo") or f"#{pid}"
        if novo=="aprovado":
            enviar_telegram(f"✅ *CADASTRO APROVADO*\n\nVendedor: {vendedor}\nCliente: {cliente}\nProtocolo: `{proto}`\n\n_Aguardando assinatura._")
        else:
            probs=[r for r in resultado["regras"] if r["resultado"] in("reprovado","pendente","alerta")]
            linhas="\n".join(f"{'❌' if r['resultado']=='reprovado' else '⚠️'} {r['legenda']}" for r in probs)
            st="REPROVADO" if novo=="reprovado" else "PENDENTE"
            enviar_telegram(f"{'❌' if novo=='reprovado' else '⚠️'} *CADASTRO {st}*\n\nVendedor: {vendedor}\nCliente: {cliente}\nProtocolo: `{proto}`\n\n*Pendências:*\n{linhas}")
    conn.close(); log.info("Auditoria concluída.")

if __name__ == "__main__": processar()
