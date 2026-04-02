"""Hub Comercial — app/routes/auth.py"""
import sqlite3, logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from pathlib import Path
from app.services.auth import verificar_senha, criar_token, get_current_user

DB_PATH = Path(__file__).resolve().parent.parent.parent / "hub_comercial.db"
log = logging.getLogger(__name__); router = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False); c.row_factory = sqlite3.Row
    try: yield c
    finally: c.close()

class LoginPayload(BaseModel):
    login: str
    senha: str

@router.post("/login")
async def login(p: LoginPayload, db=Depends(get_db)):
    u = db.execute("""
        SELECT u.id,u.nome,u.login,u.senha_hash,u.ixc_funcionario_id,g.nivel,g.nome AS grupo
        FROM hc_usuarios u JOIN hc_grupos g ON g.id=u.id_grupo
        WHERE u.login=? AND u.ativo=1""", (p.login,)).fetchone()
    if not u or not verificar_senha(p.senha, u["senha_hash"]):
        raise HTTPException(401, "Login ou senha inválidos.")
    db.execute("UPDATE hc_usuarios SET ultimo_acesso=datetime('now','-3 hours') WHERE id=?", (u["id"],))
    db.commit()
    return {"token": criar_token(u["id"],u["login"],u["nivel"],u["ixc_funcionario_id"]),
            "usuario": dict(id=u["id"],nome=u["nome"],login=u["login"],grupo=u["grupo"],nivel=u["nivel"],ixc_funcionario_id=u["ixc_funcionario_id"])}

@router.get("/me")
async def me(user=Depends(get_current_user)): return user
