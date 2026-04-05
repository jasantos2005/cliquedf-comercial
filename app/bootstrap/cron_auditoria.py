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
        SELECT p.*, COALESCE(v.nome, 'Vendedor') AS vendedor_nome,
               v.ixc_funcionario_id AS vend_func_id
        FROM hc_precadastros p
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
        # Buscar nome real no IXC se possivel
        try:
            from app.services.ixc_db import ixc_conn as _ixc
            with _ixc() as _c:
                with _c.cursor() as _icur:
                    _icur.execute("SELECT funcionario FROM ixcprovedor.funcionarios WHERE id=%s", (p.get('vend_func_id'),))
                    _fr = _icur.fetchone()
                    vendedor = (_fr['funcionario'] if _fr else p.get('vendedor_nome') or 'Vendedor').upper()
        except:
            vendedor = (p.get('vendedor_nome') or 'Vendedor').upper()
        cliente=(p.get("razao") or "—").upper()
        proto=p.get("protocolo") or f"#{pid}"
        if novo=="aprovado":
            # Criar lead no IXC
            try:
                from app.services.ixc_db import ixc_conn
                fone = (p.get('telefone_celular') or '').strip()
                whats = (p.get('whatsapp') or fone).strip()
                with ixc_conn() as ixc:
                    with ixc.cursor() as icur:
                        icur.execute("""
                            INSERT INTO ixcprovedor.contato
                            (id_contato_tipo, id_cliente, id_fornecedor, nome, fone_celular,
                             fone_whatsapp, email, principal, lid, lead, id_responsavel,
                             endereco, numero, bairro, cidade, uf, cep, cnpj_cpf,
                             data_nascimento, tipo_pessoa, ativo, id_filial,
                             data_cadastro, ultima_atualizacao, id_candidato_tipo,
                             id_segmento, tipo_localidade, origem)
                            VALUES
                            (0, 0, 0, %s, %s,
                             %s, %s, 'S', 'N', 'N', %s,
                             %s, %s, %s, %s, %s, %s, %s,
                             %s, %s, 'S', 1,
                             NOW(), NOW(), 0,
                             0, 'U', 'hub_comercial')
                        """, (
                            p.get('razao',''),
                            fone, whats,
                            p.get('email',''),
                            conn.execute('SELECT ixc_funcionario_id FROM hc_usuarios WHERE id=?',(p.get('id_vendedor_hub'),)).fetchone()[0] or 27,
                            p.get('endereco',''),
                            p.get('numero',''),
                            p.get('bairro',''),
                            p.get('ixc_cidade_id') or 0,
                            p.get('ixc_uf_id') or 7,
                            p.get('cep',''),
                            p.get('cnpj_cpf',''),
                            p.get('data_nascimento') or None,
                            p.get('tipo_pessoa','F'),
                        ))
                        ixc.commit()
                        lead_id = icur.lastrowid
                        cur.execute("UPDATE hc_precadastros SET obs=? WHERE id=?",
                            (f"lead_ixc_id={lead_id}", pid))
                        conn.commit()
                        log.info(f"#{pid} Lead IXC criado id={lead_id}")
            except Exception as e:
                log.error(f"#{pid} Erro ao criar lead IXC: {e}")
            enviar_telegram(f"✅ *CADASTRO APROVADO*\n\nVendedor: {vendedor}\nCliente: {cliente}\nProtocolo: `{proto}`\n\n_Lead criado no IXC. Aguardando consulta Serasa._")
        else:
            probs=[r for r in resultado["regras"] if r["resultado"] in("reprovado","pendente","alerta")]
            linhas="\n".join(f"{'❌' if r['resultado']=='reprovado' else '⚠️'} {r['legenda']}" for r in probs)
            st="REPROVADO" if novo=="reprovado" else "PENDENTE"
            enviar_telegram(f"{'❌' if novo=='reprovado' else '⚠️'} *CADASTRO {st}*\n\nVendedor: {vendedor}\nCliente: {cliente}\nProtocolo: `{proto}`\n\n*Pendências:*\n{linhas}")
    conn.close(); log.info("Auditoria concluída.")


def _salvar_log(status, resumo, duracao=0):
    try:
        import sqlite3, io
        DB = str(Path(__file__).resolve().parent.parent.parent / "hub_comercial.db")
        c = sqlite3.connect(DB, check_same_thread=False)
        c.execute("INSERT INTO hc_automacoes_log(motor,status,resumo,duracao_s) VALUES(?,?,?,?)",
                  ("Auditoria", status, resumo, round(duracao,2)))
        c.commit(); c.close()
    except: pass

if __name__ == "__main__": processar()
