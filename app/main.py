"""Hub Comercial — app/main.py"""
import os, logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI(title="Hub Comercial", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

from app.routes import auth, vendedor, vendedor_acompanhamento, auditoria, painel, assinatura, admin, upgrade
from app.routes.retencao import router as retencao_router, init_retencao_tables
app.include_router(auth.router,       prefix="/api/auth",       tags=["Auth"])
app.include_router(vendedor.router,   prefix="/api/vendedor",   tags=["Vendedor"])
app.include_router(vendedor_acompanhamento.router, prefix="/api/vendedor", tags=["Vendedor"])
app.include_router(auditoria.router,  prefix="/api/auditoria",  tags=["Auditoria"])
app.include_router(painel.router,     prefix="/api/painel",     tags=["Painel"])
app.include_router(assinatura.router, prefix="/api/assinatura", tags=["Assinatura"])

UPLOAD_DIR = BASE_DIR / "uploads"; UPLOAD_DIR.mkdir(exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    p = STATIC_DIR / "painel.html"
    return p.read_text() if p.exists() else "<h2>Hub Comercial OK — frontend em construção</h2>"

@app.get("/app", response_class=HTMLResponse)
async def app_vendedor():
    p = STATIC_DIR / "app.html"
    return p.read_text() if p.exists() else "<h2>App vendedor — em construção</h2>"

@app.get("/assinar/{token}", response_class=HTMLResponse)
async def pagina_assinatura(token: str):
    p = STATIC_DIR / "assinatura.html"
    return p.read_text() if p.exists() else "<h2>Assinatura — em construção</h2>"

@app.get("/health")
async def health():
    return {"status":"ok","operacao":os.getenv("OPERACAO","HubComercial"),"versao":"1.0.0"}
app.include_router(admin.router,       prefix="/api/admin",       tags=["Admin"])
app.include_router(upgrade.router,     prefix="/api/upgrade",     tags=["Upgrade"])
app.include_router(retencao_router)
init_retencao_tables()
