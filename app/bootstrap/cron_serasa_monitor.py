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
BASE_URL         = os.getenv("BASE_URL","https://comercial.iatechhub.com.br")
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

def enviar_email(destinatario, nome_cliente, plano, link):
    import smtplib, os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    smtp_host = os.getenv("SMTP_HOST","smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT",587))
    smtp_user = os.getenv("SMTP_USER","")
    smtp_pass = os.getenv("SMTP_PASS","")
    smtp_from = os.getenv("SMTP_FROM", smtp_user)
    smtp_name = os.getenv("SMTP_FROM_NAME","Cliquedf Telecom")
    if not smtp_user or not smtp_pass or not destinatario: return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Seu contrato está pronto para assinatura!"
        msg["From"]    = f"{smtp_name} <{smtp_from}>"
        msg["To"]      = destinatario
        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
          <div style="background:#1a1a2e;padding:20px;border-radius:8px;text-align:center;margin-bottom:20px;">
            <h1 style="color:#fff;margin:0;font-size:22px;">Cliquedf Telecom</h1>
          </div>
          <h2 style="color:#333;">Olá, {nome_cliente.split()[0]}! 👋</h2>
          <p style="color:#555;font-size:16px;">Seu cadastro foi <strong>aprovado</strong>! Para concluir a contratação do plano <strong>{plano}</strong>, clique no botão abaixo para assinar seu contrato digital:</p>
          <div style="text-align:center;margin:30px 0;">
            <a href="{link}" style="background:#00e5a0;color:#000;padding:14px 32px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:16px;">✍️ Assinar contrato agora</a>
          </div>
          <p style="color:#888;font-size:13px;">O link expira em 48 horas. Caso precise de ajuda, entre em contato conosco.</p>
          <hr style="border:none;border-top:1px solid #eee;margin:20px 0;">
          <p style="color:#aaa;font-size:12px;text-align:center;">Cliquedf Telecom · Atendimento ao cliente</p>
        </div>
        """
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_from, destinatario, msg.as_string())
        log.info(f"Email enviado para {destinatario}")
    except Exception as e:
        log.error(f"Erro ao enviar email: {e}")

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
        # Se foi liberado por supervisor, pular Serasa e ir direto para assinatura
        if 'liberado_supervisor=1' in (p.get('obs') or ''):
            token = gerar_link(pid, conn)
            conn.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'SUPERVISOR_BYPASS','Bypass Serasa por supervisor','ok','Liberado por supervisor')",(pid,))
            conn.commit()
            link = f"{BASE_URL}/assinar/{token}"
            telegram(TELEGRAM_CHAT_ID, f"✅ *LINK GERADO — LIBERAÇÃO SUPERVISOR*\n\nCliente: *{p.get('razao','?')}*\nVendedor: {p.get('vendedor_nome','?')}\nPlano: {p.get('plano_nome','?')}\n\n📝 [Link de assinatura]({link})")
            fone = ''.join(filter(str.isdigit, p.get('whatsapp') or p.get('telefone_celular') or ''))
            if fone:
                if not fone.startswith('55'): fone = '55' + fone
                import urllib.parse
                wa_msg = f"Olá, {p.get('razao','').split()[0]}! 😊\n\nSeu cadastro foi aprovado! Acesse o link para assinar seu contrato:\n\n{link}\n\nO link expira em 48 horas."
                wa_link = f"https://wa.me/{fone}?text={urllib.parse.quote(wa_msg)}"
                telegram(TELEGRAM_CHAT_ID, f"📱 *LINK WHATSAPP*\n\nCliente: *{p.get('razao','?')}*\nFone: {p.get('whatsapp') or p.get('telefone_celular','?')}\n\n[Clique para abrir WhatsApp]({wa_link})")
            email_cliente = (p.get('email') or '').strip()
            if email_cliente:
                enviar_email(email_cliente, p.get('razao',''), p.get('plano_nome',''), link)
            log.info(f"#{pid} LIBERADO SUPERVISOR — link gerado")
            continue
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
            # Enviar link via WhatsApp para o cliente
            fone = (p.get('whatsapp') or p.get('telefone_celular') or '').strip()
            fone = ''.join(filter(str.isdigit, fone))
            if fone:
                if not fone.startswith('55'):
                    fone = '55' + fone
                wa_msg = (
                    f"Olá, {p.get('razao','').split()[0]}! 😊\n\n"
                    f"Seu cadastro foi aprovado! Para concluir a contratação do plano "
                    f"*{p.get('plano_nome','?')}*, acesse o link abaixo para assinar seu contrato:\n\n"
                    f"{link}\n\n"
                    f"O link expira em 48 horas. Qualquer dúvida, fale com nosso time! 🚀"
                )
                import urllib.parse
                wa_link = f"https://wa.me/{fone}?text={urllib.parse.quote(wa_msg)}"
                telegram(TELEGRAM_CHAT_ID,
                    f"📱 *LINK WHATSAPP DO CLIENTE*\n\n"
                    f"Cliente: *{p.get('razao','?')}*\n"
                    f"Fone: {p.get('whatsapp') or p.get('telefone_celular','?')}\n\n"
                    f"[Clique para abrir WhatsApp]({wa_link})")
                log.info(f"#{pid} WhatsApp link gerado para {fone}")
            # Enviar email para o cliente
            email_cliente = p.get('email','').strip()
            if email_cliente:
                enviar_email(email_cliente, p.get('razao',''), p.get('plano_nome',''), link)
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


def _salvar_log(status, resumo, duracao=0):
    try:
        import sqlite3, io
        DB = str(Path(__file__).resolve().parent.parent.parent / "hub_comercial.db")
        c = sqlite3.connect(DB, check_same_thread=False)
        c.execute("INSERT INTO hc_automacoes_log(motor,status,resumo,duracao_s) VALUES(?,?,?,?)",
                  ("Monitor Serasa", status, resumo, round(duracao,2)))
        c.commit(); c.close()
    except: pass

if __name__ == "__main__": processar()
