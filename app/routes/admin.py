"""Hub Comercial — app/routes/admin.py"""
import sqlite3, hashlib, logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
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
