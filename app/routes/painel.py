"""Hub Comercial — painel.py (stub — Fase 5)"""
from fastapi import APIRouter; router = APIRouter()
@router.get("/resumo")
async def resumo(): return {"msg":"Fase 5"}
