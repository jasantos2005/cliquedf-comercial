import sqlite3, logging, sys, time, io, json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))

DB_PATH = BASE_DIR / "hub_comercial.db"

log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.StreamHandler(log_stream)]
)
log = logging.getLogger(__name__)

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def salvar_log(status, resumo, duracao):
    try:
        conn = get_db()
        log_texto = log_stream.getvalue()[-8000:]
        linhas = len(log_texto.splitlines())
        conn.execute(
            "INSERT INTO hc_automacoes_log(motor,status,linhas,resumo,log_texto,duracao_s) VALUES(?,?,?,?,?,?)",
            ("Auditoria IXC", status, linhas, resumo, log_texto, round(duracao,2))
        )
        conn.commit(); conn.close()
    except Exception as e:
        log.error(f"Erro ao salvar log: {e}")

def rodar():
    t0 = time.time()
    log.info("=== Auditoria IXC iniciada ===")
    try:
        from app.engines.auditoria_ixc_engine import auditar_contratos, resumo_auditoria
        lista = auditar_contratos("2026-01-01")
        res   = resumo_auditoria("2026-01-01")

        criticos = res["por_nivel"]["critico"]
        graves   = res["por_nivel"]["grave"]
        alertas  = res["por_nivel"]["alerta"]
        total    = res["total_problemas"]

        log.info(f"Total contratos com problemas: {total}")
        log.info(f"Criticos: {criticos} | Graves: {graves} | Alertas: {alertas}")
        for r in res["por_regra"]:
            log.info(f"  {r['regra']} — {r['legenda']}: {r['total']}")

        # Salvar snapshot no SQLite para cache
        conn = get_db()
        conn.execute("DELETE FROM hc_config WHERE chave='auditoria_ixc_cache'")
        conn.execute(
            "INSERT INTO hc_config(chave, valor, descricao) VALUES(?,?,?)",
            ("auditoria_ixc_cache", json.dumps(res), "Cache auditoria IXC — atualizado pelo cron")
        )
        conn.execute("DELETE FROM hc_config WHERE chave='auditoria_ixc_ultima'")
        conn.execute(
            "INSERT INTO hc_config(chave, valor, descricao) VALUES(?,?,?)",
            ("auditoria_ixc_ultima", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Ultima sincronizacao auditoria IXC")
        )
        conn.commit(); conn.close()

        duracao = time.time() - t0
        resumo = f"{total} problemas | {criticos} criticos | {graves} graves | {alertas} alertas | {duracao:.1f}s"
        log.info(f"=== Auditoria IXC concluida: {resumo} ===")
        salvar_log("ok", resumo, duracao)

    except Exception as e:
        duracao = time.time() - t0
        log.error(f"ERRO: {e}")
        salvar_log("erro", str(e), duracao)

if __name__ == "__main__":
    rodar()
