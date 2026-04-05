"""Hub Comercial — app/routes/admin.py"""
import sqlite3, hashlib, logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from app.services.auth import requer_supervisor, requer_admin

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "hub_comercial.db"
log      = logging.getLogger(__name__)
router   = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    try: yield c
    finally: c.close()

def h(s): return hashlib.sha256(s.encode()).hexdigest()

@router.get("/usuarios")
async def listar(db=Depends(get_db), user=Depends(requer_supervisor())):
    rows = db.execute("""
        SELECT u.id, u.nome, u.login, u.ativo, u.ixc_funcionario_id,
               g.nome AS grupo, g.nivel, u.ultimo_acesso
        FROM hc_usuarios u JOIN hc_grupos g ON g.id=u.id_grupo
        ORDER BY g.nivel DESC, u.nome
    """).fetchall()
    return {"usuarios": [dict(r) for r in rows]}

class NovoUsuario(BaseModel):
    nome: str
    login: str
    senha: str
    nivel: int = 10
    ixc_funcionario_id: Optional[int] = None

@router.post("/usuarios")
async def criar(payload: NovoUsuario, db=Depends(get_db), user=Depends(requer_admin())):
    gid = db.execute("SELECT id FROM hc_grupos WHERE nivel=?", (payload.nivel,)).fetchone()
    if not gid: raise HTTPException(400, "Nível inválido.")
    try:
        db.execute("""
            INSERT INTO hc_usuarios(nome,login,senha_hash,id_grupo,ixc_funcionario_id,ativo)
            VALUES(?,?,?,?,?,1)
        """, (payload.nome, payload.login, h(payload.senha), gid["id"], payload.ixc_funcionario_id))
        db.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(400, f"Erro: {e}")

@router.delete("/usuarios/{id}")
async def desativar(id: int, db=Depends(get_db), user=Depends(requer_admin())):
    db.execute("UPDATE hc_usuarios SET ativo=0 WHERE id=?", (id,))
    db.commit()
    return {"ok": True}


class SolicitacaoAcesso(BaseModel):
    nome: str
    login: str
    senha: str
    motivo: str



@router.put("/usuarios/{id}")
async def editar(id: int, request: Request, db=Depends(get_db), user=Depends(requer_supervisor())):
    payload = await request.json()
    nome  = payload.get("nome", "").strip()
    login = payload.get("login", "").strip()
    nivel = payload.get("nivel")
    ixc   = payload.get("ixc_funcionario_id")
    if not nome or not login:
        raise HTTPException(400, "Nome e login obrigatorios")
    db.execute(
        "UPDATE hc_usuarios SET nome=?, login=?, id_grupo=(SELECT id FROM hc_grupos WHERE nivel=? LIMIT 1), ixc_funcionario_id=? WHERE id=?",
        (nome, login, nivel, ixc, id)
    )
    db.commit()
    return {"ok": True}

@router.post("/usuarios/{id}/senha")
async def redefinir_senha(id: int, request: Request, db=Depends(get_db), user=Depends(requer_supervisor())):
    import hashlib
    payload = await request.json()
    senha = payload.get("senha", "").strip()
    if len(senha) < 8:
        raise HTTPException(400, "Senha deve ter no minimo 8 caracteres")
    hash_senha = hashlib.sha256(senha.encode()).hexdigest()
    db.execute("UPDATE hc_usuarios SET senha_hash=? WHERE id=?", (hash_senha, id))
    db.commit()
    return {"ok": True}

@router.patch("/usuarios/{id}/ativo")
async def toggle_ativo(id: int, request: Request, db=Depends(get_db), user=Depends(requer_supervisor())):
    payload = await request.json()
    ativo = 1 if payload.get("ativo") else 0
    db.execute("UPDATE hc_usuarios SET ativo=? WHERE id=?", (ativo, id))
    db.commit()
    return {"ok": True}

@router.post("/solicitar-acesso")
async def solicitar_acesso(payload: SolicitacaoAcesso, db=Depends(get_db)):
    """Endpoint público — cria usuário inativo aguardando aprovação."""
    if len(payload.senha) < 8:
        raise HTTPException(400, "Senha deve ter no mínimo 8 caracteres.")
    existe = db.execute("SELECT id FROM hc_usuarios WHERE login=?", (payload.login,)).fetchone()
    if existe:
        raise HTTPException(400, "Login já cadastrado.")
    gid = db.execute("SELECT id FROM hc_grupos WHERE nivel=10").fetchone()
    if not gid:
        raise HTTPException(500, "Grupo não encontrado.")
    try:
        db.execute("""
            INSERT INTO hc_usuarios(nome, login, senha_hash, id_grupo, ativo)
            VALUES(?, ?, ?, ?, 0)
        """, (payload.nome, payload.login, h(payload.senha), gid["id"]))
        db.commit()
        log.info(f"Solicitação de acesso: {payload.nome} ({payload.login}) — motivo: {payload.motivo}")
        return {"ok": True, "msg": "Solicitação enviada com sucesso."}
    except Exception as e:
        raise HTTPException(400, f"Erro: {e}")
