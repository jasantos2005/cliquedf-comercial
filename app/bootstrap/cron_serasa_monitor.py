"""Hub Comercial — cron_serasa_monitor.py"""
import sqlite3, os, logging, sys
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))

from app.services.credito_service import consultar_cpf

DB_PATH          = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BASE_URL         = os.getenv("BASE_URL","https://comercial.iatechhub.cloud")
IXC_URL          = os.getenv("IXC_API_URL","")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row; return c

def telegram(chat_id, msg):
    if not TELEGRAM_TOKEN or not chat_id: return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":chat_id,"text":msg,"parse_mode":"Markdown"},timeout=10)
    except Exception as e: log.warning(f"Telegram: {e}")

def gerar_link(pid, conn):
    from app.engines.contrato_engine import gerar_token_assinatura
    tk = gerar_token_assinatura(pid)
    conn.execute("""UPDATE hc_precadastros SET token_assinatura=?,token_expira_em=?,
        status='assinatura_pendente',atualizado_em=datetime('now','-3 hours') WHERE id=?""",
        (tk["token"],tk["expira_em"],pid))
    conn.commit(); return tk["token"]

def processar():
    conn = get_db()
    pendentes = conn.execute("""
        SELECT p.*, v.nome AS vendedor_nome
        FROM hc_precadastros p
        LEFT JOIN hc_usuarios u ON u.id=p.id_vendedor_hub
        LEFT JOIN hc_vendedores v ON v.id=u.ixc_funcionario_id
        WHERE p.status='aprovado' AND p.token_assinatura IS NULL
        ORDER BY p.atualizado_em ASC LIMIT 10
    """).fetchall()

    if not pendentes:
        log.info("Nenhum cadastro aguardando Serasa."); conn.close(); return

    log.info(f"Verificando {len(pendentes)} cadastro(s)...")

    for row in pendentes:
        p = dict(row); pid = p["id"]
        resultado = consultar_cpf(p.get("cnpj_cpf",""))

        if resultado["status"] == "nao_consultado":
            ja = conn.execute("SELECT id FROM hc_auditoria_log WHERE precadastro_id=? AND regra='SERASA_AGUARDANDO'",(pid,)).fetchone()
            if not ja:
                conn.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'SERASA_AGUARDANDO','Aguardando consulta Serasa','pendente','Notificado backoffice')",(pid,))
                conn.commit()
                cpf = p.get("cnpj_cpf","")
                cpf_fmt = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}" if len(cpf)==11 else cpf
                telegram(TELEGRAM_CHAT_ID,
                    f"🔍 *CONSULTA SERASA NECESSÁRIA*\n\nCliente: *{p.get('razao','?')}*\nCPF: `{cpf_fmt}`\nVendedor: {p.get('vendedor_nome','?')}\nPlano: {p.get('plano_nome','?')}\n\n[Abrir IXC]({IXC_URL})\n\n_O sistema verificará automaticamente em 2 minutos._")
                log.info(f"#{pid} — backoffice notificado")
            else:
                log.info(f"#{pid} — aguardando Serasa")
            continue

        if resultado["status"] == "erro":
            log.error(f"#{pid} erro: {resultado['msg']}"); continue

        risco = resultado.get("risco",{}); nivel = risco.get("nivel","baixo")
        ocorr = resultado.get("ocorrencias",0); valor = resultado.get("valor_total",0.0)
        log.info(f"#{pid} {p.get('razao','?')[:25]} — {ocorr} ocorr R${valor} nivel={nivel}")

        if nivel in ("baixo","medio"):
            token = gerar_link(pid, conn)
            conn.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'SERASA_OK','Serasa aprovado','ok',?)",(pid,f"{ocorr} ocorr R${valor:.2f} nivel={nivel}"))
            conn.commit()
            link = f"{BASE_URL}/assinar/{token}"
            telegram(TELEGRAM_CHAT_ID,
                f"✅ *CADASTRO APROVADO*\n\nCliente: *{p.get('razao','?')}*\nVendedor: {p.get('vendedor_nome','?')}\nPlano: {p.get('plano_nome','?')}\n\n*Serasa:* {'Sem restrições' if ocorr==0 else f'{ocorr} ocorrência(s) R$ {valor:,.2f}'}\n\n📝 [Link de assinatura]({link})")
            log.info(f"#{pid} APROVADO — link gerado")
        else:
            motivo = f"{ocorr} ocorrência(s) no Serasa — R$ {valor:,.2f} em débitos"
            conn.execute("UPDATE hc_precadastros SET status='reprovado',atualizado_em=datetime('now','-3 hours') WHERE id=?",(pid,))
            conn.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'SERASA_REPROVADO','Reprovado pelo Serasa','reprovado',?)",(pid,motivo))
            conn.commit()
            telegram(TELEGRAM_CHAT_ID,
                f"❌ *CADASTRO REPROVADO*\n\nCliente: *{p.get('razao','?')}*\nVendedor: {p.get('vendedor_nome','?')}\nMotivo: {motivo}")
            log.info(f"#{pid} REPROVADO — {motivo}")

    conn.close(); log.info("Monitor Serasa concluído.")

if __name__ == "__main__": processar()
