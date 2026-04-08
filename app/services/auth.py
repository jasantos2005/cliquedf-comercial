"""
Hub Comercial — app/services/auth.py
JWT + níveis: vendedor(10) backoffice(30) supervisor(50) admin(99)
"""
import hashlib, os, logging
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
log    = logging.getLogger(__name__)
SECRET = os.getenv("SECRET_KEY", "dev_key")
ALGO   = "HS256"
bearer = HTTPBearer(auto_error=False)

def hash_senha(s): return hashlib.sha256(s.encode()).hexdigest()
def verificar_senha(s, h): return hash_senha(s) == h

def criar_token(user_id, login, nivel, ixc_funcionario_id=None):
    return jwt.encode({"sub": str(user_id), "login": login, "nivel": nivel,
        "ixc_funcionario_id": ixc_funcionario_id,
        "exp": datetime.utcnow() + timedelta(days=30)}, SECRET, algorithm=ALGO)

def get_current_user(c: HTTPAuthorizationCredentials = Depends(bearer)):
    if not c: raise HTTPException(401, "Token não fornecido.")
    try: return jwt.decode(c.credentials, SECRET, algorithms=[ALGO])
    except JWTError as e: raise HTTPException(401, f"Token inválido: {e}")

def requer_nivel(n):
    def check(user=Depends(get_current_user)):
        if user.get("nivel",0) < n: raise HTTPException(403, "Acesso negado.")
        return user
    return check

def requer_vendedor():  return requer_nivel(10)
def requer_backoffice(): return requer_nivel(30)
def requer_supervisor(): return requer_nivel(50)
def requer_admin():      return requer_nivel(99)
