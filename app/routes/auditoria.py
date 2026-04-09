"""Hub Comercial — app/routes/auditoria.py"""
import sqlite3, logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from app.services.auth import requer_backoffice, requer_supervisor

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "hub_comercial.db"
log = logging.getLogger(__name__); router = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False); c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    try: yield c
    finally: c.close()

def agora(): 
    from datetime import datetime; return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@router.get("/resumo")
async def resumo(db=Depends(get_db), user=Depends(requer_backoffice())):
    r = db.execute("""SELECT COUNT(*) AS total,SUM(status='enviado') AS enviados,
        SUM(status='em_auditoria') AS em_auditoria,SUM(status='aprovado') AS aprovados,
        SUM(status='pendente') AS pendentes,SUM(status='reprovado') AS reprovados,
        SUM(status='assinatura_pendente') AS aguard_assinatura,SUM(status='ativado') AS ativados,
        SUM(date(criado_em)=date(datetime('now','-3 hours'))) AS hoje
        FROM hc_precadastros""").fetchone()
    return dict(r)

@router.get("/pendentes")
async def listar_pendentes(status:str=Query("pendente"),pagina:int=Query(1,ge=1),db=Depends(get_db),user=Depends(requer_backoffice())):
    pp=20; offset=(pagina-1)*pp
    rows = db.execute("""
        SELECT p.id,p.protocolo,p.status,p.razao,p.cnpj_cpf,p.tipo_pessoa,p.telefone_celular,p.email,
               p.cidade_nome,p.uf_sigla,p.bairro,p.plano_nome,p.plano_valor,p.canal_venda,
               p.viabilidade_status,p.criado_em,p.atualizado_em,v.nome AS vendedor_nome,
               (SELECT COUNT(*) FROM hc_auditoria_log a WHERE a.precadastro_id=p.id AND a.resultado='reprovado') AS qtd_reprovadas,
               (SELECT COUNT(*) FROM hc_auditoria_log a WHERE a.precadastro_id=p.id AND a.resultado='pendente')  AS qtd_pendentes,
               (SELECT COUNT(*) FROM hc_precadastro_docs d WHERE d.precadastro_id=p.id) AS qtd_docs
        FROM hc_precadastros p LEFT JOIN hc_usuarios v ON v.id=p.id_vendedor_hub
        WHERE p.status=? ORDER BY p.criado_em DESC LIMIT ? OFFSET ?
    """, (status,pp,offset)).fetchall()
    total = db.execute("SELECT COUNT(*) FROM hc_precadastros WHERE status=?",(status,)).fetchone()[0]
    return {"pagina":pagina,"por_pagina":pp,"total":total,"cadastros":[dict(r) for r in rows]}


# ── LIBERAÇÕES ────────────────────────────────────────────────
from pydantic import BaseModel as _BM

class LiberacaoPayload(_BM):
    motivo: str

@router.get("/liberacoes")
async def listar_liberacoes(db=Depends(get_db), user=Depends(requer_supervisor())):
    rows = db.execute("""
        SELECT p.id, p.razao, p.cnpj_cpf, p.status, p.plano_nome, p.plano_valor,
               p.cidade_nome, p.uf_sigla, p.telefone_celular, p.whatsapp, p.email,
               p.canal_venda, p.atualizado_em, p.criado_em,
               u.nome AS vendedor_nome,
               (SELECT GROUP_CONCAT(a.legenda, ' | ')
                FROM hc_auditoria_log a
                WHERE a.precadastro_id = p.id AND a.resultado = 'reprovado'
                ORDER BY a.id DESC LIMIT 5) AS motivos_reprovacao
        FROM hc_precadastros p
        LEFT JOIN hc_usuarios u ON u.id = p.id_vendedor_hub
        WHERE p.status = 'reprovado'
        ORDER BY p.atualizado_em DESC LIMIT 50
    """).fetchall()
    return {"liberacoes": [dict(r) for r in rows]}

@router.post("/liberacoes/{id}/aprovar")
async def liberar_aprovar(id: int, payload: LiberacaoPayload, db=Depends(get_db), user=Depends(requer_supervisor())):
    import os, requests as _req
    u = db.execute("SELECT nome FROM hc_usuarios WHERE id=?", (user["sub"],)).fetchone()
    liberador = u["nome"] if u else "Supervisor"
    pre = db.execute("SELECT p.*, u.nome AS vendedor_nome FROM hc_precadastros p LEFT JOIN hc_usuarios u ON u.id=p.id_vendedor_hub WHERE p.id=?", (id,)).fetchone()
    if not pre: raise HTTPException(404, "Não encontrado")
    pre = dict(pre)
    if pre["status"] != "reprovado": raise HTTPException(400, "Cadastro não está reprovado")
    db.execute("UPDATE hc_precadastros SET status='aprovado', obs=COALESCE(obs||' ','')|| 'liberado_supervisor=1', atualizado_em=datetime('now','-3 hours') WHERE id=?", (id,))
    db.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes) VALUES(?,99,'LIBERACAO','Liberado por supervisor','aprovado',?)",
        (id, f"Liberado por {liberador}: {payload.motivo}"))
    db.commit()
    token = os.getenv("TELEGRAM_TOKEN"); chat = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat:
        try:
            _req.post(f"https://api.telegram.org/bot{token}/sendMessage", json={
                "chat_id": chat,
                "text": f"✅ *VENDA LIBERADA*\n\nCliente: *{pre.get('razao','?')}*\nVendedor: {pre.get('vendedor_nome') or ''}\nLiberado por: {liberador}\nMotivo: {payload.motivo}",
                "parse_mode": "Markdown"}, timeout=10)
        except: pass
    return {"ok": True}

@router.get("/{id}")
async def detalhe(id:int, db=Depends(get_db), user=Depends(requer_backoffice())):
    p = db.execute("SELECT p.*,v.nome AS vendedor_nome FROM hc_precadastros p LEFT JOIN hc_usuarios v ON v.id=p.id_vendedor_hub WHERE p.id=?",(id,)).fetchone()
    if not p: raise HTTPException(404,"Não encontrado.")
    docs     = db.execute("SELECT id,tipo,arquivo,tamanho_kb,criado_em FROM hc_precadastro_docs WHERE precadastro_id=? ORDER BY id",(id,)).fetchall()
    auditoria= db.execute("SELECT regra,legenda,resultado,detalhes,criado_em FROM hc_auditoria_log WHERE precadastro_id=? ORDER BY rodada DESC,id ASC",(id,)).fetchall()
    ativacao = db.execute("SELECT etapa,sucesso,erro_msg,ixc_id_gerado,tentativa,criado_em FROM hc_ativacoes_log WHERE precadastro_id=? ORDER BY id DESC",(id,)).fetchall()
    return {"precadastro":dict(p),"docs":[dict(d) for d in docs],"auditoria":[dict(a) for a in auditoria],"ativacao":[dict(a) for a in ativacao]}

class AprovarPayload(BaseModel):
    justificativa: str = ""

@router.post("/{id}/aprovar")
async def aprovar(id:int, payload:AprovarPayload, db=Depends(get_db), user=Depends(requer_supervisor())):
    p = db.execute("SELECT status FROM hc_precadastros WHERE id=?",(id,)).fetchone()
    if not p: raise HTTPException(404,"Não encontrado.")
    if p["status"] not in("pendente","reprovado","em_auditoria","aguard_correcao"): raise HTTPException(400,f"Status '{p['status']}' não permite aprovação.")
    db.execute("UPDATE hc_precadastros SET status='aprovado',atualizado_em=datetime('now','-3 hours') WHERE id=?",(id,))
    db.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'MANUAL','Aprovação manual','ok',?)",
        (id,f"Aprovado por {user['login']} — {payload.justificativa}"))
    db.commit()
    return {"ok":True,"status":"aprovado"}

class ReprovarPayload(BaseModel):
    motivo: str

@router.post("/{id}/reprovar")
async def reprovar(id:int, payload:ReprovarPayload, db=Depends(get_db), user=Depends(requer_supervisor())):
    if not payload.motivo.strip(): raise HTTPException(400,"Motivo obrigatório.")
    p = db.execute("SELECT status FROM hc_precadastros WHERE id=?",(id,)).fetchone()
    if not p: raise HTTPException(404,"Não encontrado.")
    if p["status"] in("ativado","assinado"): raise HTTPException(400,"Não é possível reprovar cadastro já ativado.")
    db.execute("UPDATE hc_precadastros SET status='reprovado',atualizado_em=datetime('now','-3 hours') WHERE id=?",(id,))
    db.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'MANUAL','Reprovação manual','reprovado',?)",
        (id,f"Reprovado por {user['login']} — {payload.motivo}"))
    db.commit()
    return {"ok":True,"status":"reprovado"}

@router.post("/{id}/devolver")
async def devolver_correcao(id:int, db=Depends(get_db), user=Depends(requer_supervisor())):
    """
    Devolve o cadastro para o vendedor corrigir.
    Status: aguard_correcao — aparece no app do vendedor com botão Corrigir.
    """
    p = db.execute("SELECT status, razao, id_vendedor_hub FROM hc_precadastros WHERE id=?", (id,)).fetchone()
    if not p: raise HTTPException(404, "Não encontrado.")
    if p["status"] in ("ativado","assinado"): raise HTTPException(400, "Não é possível devolver cadastro já ativado.")
    db.execute("UPDATE hc_precadastros SET status='aguard_correcao',atualizado_em=datetime('now','-3 hours') WHERE id=?", (id,))
    db.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)VALUES(?,99,'DEVOLVER','Devolvido para correção','pendente',?)",
        (id, f"Devolvido por {user['login']} para correção pelo vendedor"))
    db.commit()
    # Notificar Telegram
    try:
        import os, requests as _req
        from dotenv import load_dotenv
        load_dotenv(str(BASE / ".env"))
        token   = os.getenv("TELEGRAM_TOKEN","")
        chat_id = os.getenv("TELEGRAM_CHAT_ID","")
        if token and chat_id:
            _req.post(f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id,
                      "text": f"🔄 *CORREÇÃO SOLICITADA*\n\n👤 *Cliente:* {p['razao']}\n👨‍💼 *Por:* {user['login']}\n\n_O vendedor precisa corrigir o cadastro no app._",
                      "parse_mode": "Markdown"}, timeout=5)
    except: pass
    return {"ok": True, "status": "aguard_correcao"}

@router.post("/{id}/reauditar")
async def reauditar(id:int, db=Depends(get_db), user=Depends(requer_backoffice())):
    p = db.execute("SELECT status FROM hc_precadastros WHERE id=?",(id,)).fetchone()
    if not p: raise HTTPException(404,"Não encontrado.")
    db.execute("UPDATE hc_precadastros SET status='enviado',atualizado_em=datetime('now','-3 hours') WHERE id=?",(id,))
    db.commit()
    return {"ok":True,"msg":"Reenviado para auditoria."}

# ── GERAR LINK DE ASSINATURA (atalho pelo painel de auditoria) ──
@router.post("/{id}/gerar-link")
async def gerar_link_assinatura(id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    from app.routes.assinatura import gerar_link
    return await gerar_link(id, db, user)


# ── CONSULTA DE CRÉDITO CPF/CNPJ ─────────────────────────────
@router.get("/credito/{cpf_cnpj}")
async def consultar_credito(
    cpf_cnpj: str,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    from app.services.credito_service import consultar_cpf
    return consultar_cpf(cpf_cnpj)

@router.get("/{id}/credito")
async def credito_precadastro(
    id: int,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """Consulta crédito direto pelo ID do pré-cadastro."""
    p = db.execute(
        "SELECT cnpj_cpf, razao FROM hc_precadastros WHERE id=?", (id,)
    ).fetchone()
    if not p:
        raise HTTPException(404, "Não encontrado.")
    from app.services.credito_service import consultar_cpf
    resultado = consultar_cpf(p["cnpj_cpf"])
    resultado["cliente"] = p["razao"]
    return resultado


# ── ADMIN USUARIOS (inline) ───────────────────────────────────
from fastapi import Body
@router.get("/../../admin/usuarios")
async def listar_usuarios_aud(db=Depends(get_db), user=Depends(requer_supervisor())):
    rows = db.execute("""
        SELECT u.id, u.nome, u.login, u.ativo, u.ixc_funcionario_id,
               g.nome AS grupo, g.nivel, u.ultimo_acesso
        FROM hc_usuarios u JOIN hc_grupos g ON g.id=u.id_grupo
        ORDER BY g.nivel DESC, u.nome
    """).fetchall()
    return {"usuarios": [dict(r) for r in rows]}


@router.post("/liberacoes/{id}/recusar")
async def liberar_recusar(id: int, payload: LiberacaoPayload, db=Depends(get_db), user=Depends(requer_supervisor())):
    import os, requests as _req
    u = db.execute("SELECT nome FROM hc_usuarios WHERE id=?", (user["sub"],)).fetchone()
    liberador = u["nome"] if u else "Supervisor"
    pre = db.execute("SELECT p.*, u.nome AS vendedor_nome FROM hc_precadastros p LEFT JOIN hc_usuarios u ON u.id=p.id_vendedor_hub WHERE p.id=?", (id,)).fetchone()
    if not pre: raise HTTPException(404, "Não encontrado")
    pre = dict(pre)
    db.execute("INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes) VALUES(?,99,'LIBERACAO_NEGADA','Recusado pelo supervisor','reprovado',?)",
        (id, f"Recusado por {liberador}: {payload.motivo}"))
    db.commit()
    token = os.getenv("TELEGRAM_TOKEN"); chat = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat:
        try:
            _req.post(f"https://api.telegram.org/bot{token}/sendMessage", json={
                "chat_id": chat,
                "text": f"❌ *VENDA RECUSADA*\n\nCliente: *{pre.get('razao','?')}*\nRecusado por: {liberador}\nMotivo: {payload.motivo}",
                "parse_mode": "Markdown"}, timeout=10)
        except: pass
    return {"ok": True}
