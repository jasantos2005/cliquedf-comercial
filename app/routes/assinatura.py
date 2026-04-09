"""
Hub Comercial — app/routes/assinatura.py
=========================================
Endpoints relacionados à assinatura digital de contratos.

Fluxo:
    1. Backoffice gera link → POST /api/assinatura/gerar-link/{id}
    2. Cliente acessa a página → GET /api/assinatura/{token}
    3. Cliente assina → POST /api/assinatura/{token}/assinar
       → salva PNG da assinatura
       → gera PDF do contrato assinado
       → chama ativar_cliente() → recebe ixc_cliente_id de retorno direto
       → atualiza status_internet no IXC para 'A'
       → envia documentos para ixcprovedor.cliente_arquivos
         usando o ixc_cliente_id do retorno (sem reler o banco)

CORREÇÃO: o ixc_cliente_id agora vem do retorno de ativar_cliente(),
          não de uma nova leitura do banco SQLite — elimina o problema
          de timing onde o commit ainda não havia propagado.
"""
import sqlite3, base64, logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from app.services.auth import requer_backoffice

BASE_DIR   = Path(__file__).resolve().parent.parent.parent
DB_PATH    = BASE_DIR / "hub_comercial.db"
UPLOAD_DIR = BASE_DIR / "uploads"
log        = logging.getLogger(__name__)
router     = APIRouter()


def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    try:
        yield c
    finally:
        c.close()


def agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Gerar link de assinatura ──────────────────────────────────

@router.post("/gerar-link/{id}")
async def gerar_link(id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    """
    Gera o token de assinatura e muda o status para 'assinatura_pendente'.
    Só funciona se o cadastro estiver com status='aprovado'.
    """
    p = db.execute(
        "SELECT status, razao FROM hc_precadastros WHERE id=?", (id,)
    ).fetchone()

    if not p:
        raise HTTPException(404, "Não encontrado.")
    if p["status"] != "aprovado":
        raise HTTPException(400, f"Cadastro deve estar aprovado. Status atual: {p['status']}")

    from app.engines.contrato_engine import gerar_token_assinatura
    tk = gerar_token_assinatura(id)

    db.execute("""
        UPDATE hc_precadastros
        SET token_assinatura=?, token_expira_em=?,
            status='assinatura_pendente',
            atualizado_em=datetime('now','-3 hours')
        WHERE id=?
    """, (tk["token"], tk["expira_em"], id))
    db.commit()

    import os
    base_url = os.getenv("BASE_URL", "https://comercial.iatechhub.com.br")
    link = f"{base_url}/assinar/{tk['token']}"
    log.info(f"Link assinatura gerado para #{id} por {user['login']}")
    return {"link": link, "expira_em": tk["expira_em"], "cliente": p["razao"]}


# ── Dados do contrato (público, requer token válido) ──────────

@router.get("/{token}")
async def get_contrato(token: str, db=Depends(get_db)):
    """
    Retorna os dados do contrato para renderização na página de assinatura.
    Endpoint público — autenticado apenas pelo token de assinatura.
    """
    p = db.execute("""
        SELECT id, razao, cnpj_cpf, plano_nome, plano_valor,
               taxa_instalacao, fidelidade, dia_vencimento,
               token_expira_em, status
        FROM hc_precadastros WHERE token_assinatura=?
    """, (token,)).fetchone()

    if not p:
        raise HTTPException(404, "Link inválido ou expirado.")
    if p["status"] != "assinatura_pendente":
        raise HTTPException(400, "Este contrato já foi assinado ou está inativo.")
    if agora() > p["token_expira_em"]:
        raise HTTPException(400, "Link expirado. Solicite um novo link ao vendedor.")

    precadastro = dict(
        db.execute("SELECT * FROM hc_precadastros WHERE token_assinatura=?", (token,)).fetchone()
    )

    from app.engines.contrato_engine import gerar_html_contrato
    html_contrato = gerar_html_contrato(precadastro)

    return {
        "cliente":       p["razao"],
        "cnpj_cpf":      p["cnpj_cpf"],
        "plano":         p["plano_nome"],
        "valor":         float(p["plano_valor"] or 0),
        "taxa":          float(p["taxa_instalacao"] or 0),
        "fidelidade":    p["fidelidade"],
        "vencimento":    p["dia_vencimento"],
        "expira_em":     p["token_expira_em"],
        "html_contrato": html_contrato,
    }


# ── Salvar assinatura e ativar no IXC ─────────────────────────

class AssinarPayload(BaseModel):
    assinatura_base64: str  # PNG em base64 gerado pelo canvas
    aceite_termos: bool


@router.post("/{token}/assinar")
async def assinar(token: str, payload: AssinarPayload, db=Depends(get_db)):
    """
    Processa a assinatura digital e dispara a ativação no IXC.

    Sequência após receber a assinatura:
        1. Salva PNG da assinatura no disco
        2. Muda status para 'assinado'
        3. Gera PDF do contrato com assinatura embutida
        4. Chama ativar_cliente() → retorna ixc_cliente_id diretamente
        5. Atualiza status_internet para 'A' no IXC
        6. Envia documentos do hub para ixcprovedor.cliente_arquivos
           usando o ixc_cliente_id retornado (sem reler o banco)
    """
    if not payload.aceite_termos:
        raise HTTPException(400, "É necessário aceitar os termos do contrato.")
    if not payload.assinatura_base64:
        raise HTTPException(400, "Assinatura não fornecida.")

    p = db.execute("""
        SELECT id, razao, status, token_expira_em
        FROM hc_precadastros WHERE token_assinatura=?
    """, (token,)).fetchone()

    if not p:
        raise HTTPException(404, "Link inválido.")
    if p["status"] != "assinatura_pendente":
        raise HTTPException(400, "Contrato já assinado ou inativo.")
    if agora() > p["token_expira_em"]:
        raise HTTPException(400, "Link expirado.")

    pid = p["id"]
    pasta = UPLOAD_DIR / str(pid)
    pasta.mkdir(exist_ok=True)

    # ── 1. Salvar PNG da assinatura ───────────────────────────
    try:
        img_data = base64.b64decode(payload.assinatura_base64.split(",")[-1])
        arquivo  = pasta / "assinatura_cliente.png"
        arquivo.write_bytes(img_data)
        arquivo_rel = f"uploads/{pid}/assinatura_cliente.png"
    except Exception as e:
        log.error(f"#{pid} Erro ao salvar assinatura: {e}")
        raise HTTPException(500, "Erro ao salvar assinatura.")

    # ── 2. Atualizar status para 'assinado' ───────────────────
    db.execute("""
        UPDATE hc_precadastros
        SET status='assinado',
            assinado_em=datetime('now','-3 hours'),
            assinatura_arquivo=?,
            atualizado_em=datetime('now','-3 hours')
        WHERE id=?
    """, (arquivo_rel, pid))
    db.commit()
    log.info(f"Contrato #{pid} assinado — {p['razao']}")

    # ── 3. Gerar PDF do contrato com assinatura embutida ──────
    try:
        from app.engines.contrato_engine import gerar_html_contrato
        import pdfkit, base64 as _b64
        pre_pdf = db.execute("SELECT * FROM hc_precadastros WHERE id=?", (pid,)).fetchone()
        if pre_pdf:
            html_contrato = gerar_html_contrato(dict(pre_pdf))
            sig_path = pasta / "assinatura_cliente.png"
            if sig_path.exists():
                sig_b64 = _b64.b64encode(sig_path.read_bytes()).decode()
                sig_tag = f'<img src="data:image/png;base64,{sig_b64}" style="max-width:300px;"/>'
                html_contrato = html_contrato.replace("___________________________", sig_tag, 1)
            pdf_path = pasta / f"contrato_{pid}.pdf"
            pdfkit.from_string(html_contrato, str(pdf_path), options={"quiet": ""})
            log.info(f"#{pid} PDF do contrato gerado: {pdf_path.name}")
    except Exception as e:
        log.error(f"#{pid} Erro ao gerar PDF: {e}")

    # ── 4. Ativar no IXC — recebe ixc_cliente_id diretamente ──
    # IMPORTANTE: usamos o retorno de ativar_cliente() em vez de reler
    # o banco SQLite, evitando problemas de timing com o commit.
    ixc_cliente_id = 0
    ixc_contrato_id = 0
    try:
        from app.engines.ativacao_engine import ativar_cliente
        ixc_cliente_id = ativar_cliente(pid)
        log.info(f"#{pid} ativar_cliente retornou ixc_cliente_id={ixc_cliente_id}")
    except Exception as e:
        log.error(f"#{pid} Erro na ativação: {e}")

    # Buscar ixc_contrato_id para atualizar status_internet
    if ixc_cliente_id:
        row_cont = db.execute(
            "SELECT ixc_contrato_id FROM hc_precadastros WHERE id=?", (pid,)
        ).fetchone()
        ixc_contrato_id = row_cont["ixc_contrato_id"] if row_cont else 0

    # ── 5. Atualizar status_internet para 'A' no IXC ─────────
    if ixc_contrato_id:
        try:
            from app.services.ixc_db import ixc_conn
            with ixc_conn() as ixc:
                with ixc.cursor() as cur:
                    cur.execute(
                        "UPDATE ixcprovedor.cliente_contrato "
                        "SET status_internet='A', ultima_atualizacao=NOW() WHERE id=%s",
                        (ixc_contrato_id,)
                    )
                    ixc.commit()
            log.info(f"#{pid} status_internet atualizado para A")
        except Exception as e:
            log.error(f"#{pid} Erro ao atualizar status_internet: {e}")

    # ── 6. Enviar documentos para ixcprovedor.cliente_arquivos ─
    # Usa ixc_cliente_id do retorno de ativar_cliente() — sem reler banco.
    if ixc_cliente_id:
        _enviar_documentos(pid, ixc_cliente_id, ixc_contrato_id, pasta)

    protocolo = db.execute(
        "SELECT protocolo FROM hc_precadastros WHERE id=?", (pid,)
    ).fetchone()["protocolo"]

    return {
        "ok":       True,
        "msg":      "Contrato assinado com sucesso! Aguarde o contato do nosso técnico para instalação.",
        "protocolo": protocolo,
    }


def _enviar_documentos(pid: int, ixc_cliente_id: int, ixc_contrato_id: int, pasta: Path):
    """
    Envia os documentos do pré-cadastro para ixcprovedor.cliente_arquivos via API REST do IXC.
    Também envia o PDF do contrato assinado.

    Chamado com o ixc_cliente_id retornado por ativar_cliente() —
    nunca rele o banco para obter este valor após a ativação.
    """
    import os, requests as _req, unicodedata as _ud, sqlite3 as _sq

    IXC_URL   = os.getenv("IXC_API_URL",   "")
    IXC_USER  = os.getenv("IXC_API_USER",  "")
    IXC_TOKEN = os.getenv("IXC_API_TOKEN", "")

    if not IXC_URL or not IXC_USER or not IXC_TOKEN:
        log.warning(f"#{pid} Envio de docs ignorado — credenciais IXC não configuradas.")
        return

    # Ler documentos e dados do pré-cadastro do banco
    try:
        _conn = _sq.connect(str(DB_PATH), check_same_thread=False)
        _conn.row_factory = _sq.Row
        pre = _conn.execute("SELECT * FROM hc_precadastros WHERE id=?", (pid,)).fetchone()
        docs = _conn.execute(
            "SELECT tipo, arquivo FROM hc_precadastro_docs WHERE precadastro_id=?", (pid,)
        ).fetchall()
        _conn.close()
    except Exception as e:
        log.error(f"#{pid} Erro ao ler docs do banco: {e}")
        return

    if not docs:
        log.info(f"#{pid} Nenhum documento para enviar.")
        return

    import base64 as _b64e
    auth = _b64e.b64encode(f"{IXC_USER}:{IXC_TOKEN}".encode()).decode()
    headers_ixc = {
        "Authorization": f"Basic {auth}",
        "ixcsoft": "gravar",
    }

    # Prefixo do nome do arquivo com o primeiro nome do cliente
    _razao = (pre["razao"] or "").split()[0].upper() if pre else ""
    razao_curta = _ud.normalize("NFKD", _razao).encode("ascii", "ignore").decode("ascii")

    # Enviar cada documento de identidade/selfie/comprovante
    for doc in docs:
        doc_path = BASE_DIR / doc["arquivo"]
        if not doc_path.exists():
            log.warning(f"#{pid} Arquivo não encontrado: {doc_path}")
            continue

        descricao = doc["tipo"].replace("_", " ").upper()
        nome_arq  = f"{descricao}_{razao_curta}{doc_path.suffix}".replace(" ", "_")
        mime = (
            "image/jpeg" if doc_path.suffix.lower() in (".jpg", ".jpeg")
            else "image/png" if doc_path.suffix.lower() == ".png"
            else "application/octet-stream"
        )

        try:
            with open(doc_path, "rb") as f_doc:
                r = _req.post(
                    f"{IXC_URL}/webservice/v1/cliente_arquivos",
                    headers=headers_ixc,
                    files={"arquivo": (doc_path.name, f_doc, mime)},
                    data={
                        "id_cliente": str(ixc_cliente_id),
                        "descricao":  descricao,
                        "local_arquivo": f"arquivos/{doc_path.name}",
                    },
                    timeout=15,
                )
            log.info(f"#{pid} doc {nome_arq} → status={r.status_code}")

            # Corrigir nome_arquivo via MySQL direto (API IXC não aceita no multipart)
            try:
                import json as _json
                resp_data = _json.loads(r.text)
                new_id = resp_data.get("id")
                if new_id:
                    from app.services.ixc_db import ixc_conn
                    with ixc_conn() as _ixc:
                        with _ixc.cursor() as _cur:
                            _cur.execute(
                                "UPDATE ixcprovedor.cliente_arquivos SET nome_arquivo=%s WHERE id=%s",
                                (nome_arq, new_id)
                            )
                        _ixc.commit()
                    log.info(f"#{pid} nome_arquivo corrigido id={new_id} → {nome_arq}")
            except Exception as _e:
                log.warning(f"#{pid} Não corrigiu nome_arquivo: {_e}")

        except Exception as e:
            log.error(f"#{pid} Erro ao enviar {nome_arq}: {e}")

    # Enviar PDF do contrato assinado
    pdf_files = sorted(pasta.glob("contrato_*.pdf")) if pasta.exists() else []
    if not pdf_files:
        pdf_files = sorted(pasta.glob("*.pdf")) if pasta.exists() else []

    if pdf_files and ixc_contrato_id:
        pdf_path = pdf_files[-1]
        descricao_pdf = f"Contrato ID.{ixc_contrato_id} com assinatura digital"
        try:
            with open(pdf_path, "rb") as f_pdf:
                r = _req.post(
                    f"{IXC_URL}/webservice/v1/cliente_arquivos",
                    headers=headers_ixc,
                    files={"arquivo": (pdf_path.name, f_pdf, "application/pdf")},
                    data={
                        "id_cliente":  str(ixc_cliente_id),
                        "descricao":   descricao_pdf,
                        "nome_arquivo": pdf_path.name,
                        "local_arquivo": f"arquivos/{pdf_path.name}",
                    },
                    timeout=15,
                )
            log.info(f"#{pid} pdf {pdf_path.name} → status={r.status_code}")
        except Exception as e:
            log.error(f"#{pid} Erro ao enviar PDF: {e}")

    log.info(f"#{pid} Envio de documentos para IXC cliente {ixc_cliente_id} concluído.")
