"""
Hub Comercial — app/routes/assinatura.py
GET  /api/assinatura/{token}          → dados do contrato para a página
POST /api/assinatura/{token}/assinar  → salva assinatura e dispara ativação
POST /api/auditoria/{id}/gerar-link   → gera link de assinatura para cadastro aprovado
"""
import sqlite3, base64, logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from app.services.auth import requer_backoffice, requer_supervisor

BASE_DIR   = Path(__file__).resolve().parent.parent.parent
DB_PATH    = BASE_DIR / "hub_comercial.db"
UPLOAD_DIR = BASE_DIR / "uploads"
log        = logging.getLogger(__name__)
router     = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row; c.execute("PRAGMA foreign_keys=ON")
    try: yield c
    finally: c.close()

def agora(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ── GERAR LINK ───────────────────────────────────────────────
@router.post("/gerar-link/{id}")
async def gerar_link(id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    p = db.execute("SELECT status, razao FROM hc_precadastros WHERE id=?", (id,)).fetchone()
    if not p: raise HTTPException(404, "Não encontrado.")
    if p["status"] not in ("aprovado",):
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

    base_url = __import__("os").getenv("BASE_URL", "https://comercial.iatechhub.cloud")
    link = f"{base_url}/assinar/{tk['token']}"
    log.info(f"Link assinatura gerado para #{id} por {user['login']}")
    return {"link": link, "expira_em": tk["expira_em"], "cliente": p["razao"]}

# ── DADOS DO CONTRATO (público — só com token válido) ────────
@router.get("/{token}")
async def get_contrato(token: str, db=Depends(get_db)):
    p = db.execute("""
        SELECT id, razao, cnpj_cpf, plano_nome, plano_valor,
               taxa_instalacao, fidelidade, dia_vencimento,
               token_expira_em, status
        FROM hc_precadastros WHERE token_assinatura=?
    """, (token,)).fetchone()

    if not p: raise HTTPException(404, "Link inválido ou expirado.")
    if p["status"] not in ("assinatura_pendente",):
        raise HTTPException(400, "Este contrato já foi assinado ou está inativo.")
    if agora() > p["token_expira_em"]:
        raise HTTPException(400, "Link expirado. Solicite um novo link ao vendedor.")

    precadastro = dict(db.execute("SELECT * FROM hc_precadastros WHERE token_assinatura=?", (token,)).fetchone())

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

# ── SALVAR ASSINATURA ────────────────────────────────────────
class AssinarPayload(BaseModel):
    assinatura_base64: str  # PNG em base64 do canvas
    aceite_termos: bool

@router.post("/{token}/assinar")
async def assinar(token: str, payload: AssinarPayload, db=Depends(get_db)):
    if not payload.aceite_termos:
        raise HTTPException(400, "É necessário aceitar os termos do contrato.")
    if not payload.assinatura_base64:
        raise HTTPException(400, "Assinatura não fornecida.")

    p = db.execute("""
        SELECT id, razao, status, token_expira_em
        FROM hc_precadastros WHERE token_assinatura=?
    """, (token,)).fetchone()

    if not p: raise HTTPException(404, "Link inválido.")
    if p["status"] != "assinatura_pendente":
        raise HTTPException(400, "Contrato já assinado ou inativo.")
    if agora() > p["token_expira_em"]:
        raise HTTPException(400, "Link expirado.")

    pid = p["id"]

    # Salvar PNG da assinatura
    pasta = UPLOAD_DIR / str(pid); pasta.mkdir(exist_ok=True)
    try:
        img_data = base64.b64decode(payload.assinatura_base64.split(",")[-1])
        arquivo  = pasta / "assinatura_cliente.png"
        arquivo.write_bytes(img_data)
        arquivo_rel = f"uploads/{pid}/assinatura_cliente.png"
    except Exception as e:
        log.error(f"Erro ao salvar assinatura #{pid}: {e}")
        raise HTTPException(500, "Erro ao salvar assinatura.")

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

    # Disparar ativação no IXC em background
    try:
        from app.engines.ativacao_engine import ativar_cliente
        ativar_cliente(pid)
    except Exception as e:
        log.error(f"Erro na ativação #{pid}: {e}")

    return {
        "ok": True,
        "msg": "Contrato assinado com sucesso! Aguarde o contato do nosso técnico para instalação.",
        "protocolo": db.execute("SELECT protocolo FROM hc_precadastros WHERE id=?", (pid,)).fetchone()["protocolo"]
    }
