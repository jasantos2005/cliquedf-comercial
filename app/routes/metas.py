"""
Hub Comercial — app/routes/metas.py
=====================================
GET  /api/metas              → lista metas do mes com progresso
POST /api/metas              → criar/atualizar meta de um vendedor
GET  /api/metas/historico    → historico de metas por vendedor
"""
import sqlite3, logging
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.services.auth import requer_backoffice, requer_vendedor

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "hub_comercial.db"
log      = logging.getLogger(__name__)
router   = APIRouter()


def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    try: yield c
    finally: c.close()


@router.get("")
async def listar_metas(
    mes: str = "",
    db=Depends(get_db),
    user=Depends(requer_vendedor())
):
    if not mes:
        mes = datetime.now().strftime("%Y-%m")

    rows = db.execute("""
        SELECT
            v.id as vendedor_id,
            v.nome as vendedor_nome,
            COALESCE(m.meta, 0) as meta,
            COALESCE(m.id, NULL) as meta_id,
            COUNT(p.id) as realizados,
            CASE WHEN COALESCE(m.meta,0) > 0
                 THEN ROUND(COUNT(p.id) * 100.0 / m.meta, 1)
                 ELSE 0 END as percentual
        FROM hc_vendedores v
        LEFT JOIN hc_metas m ON m.vendedor_id = v.id AND m.mes = ?
        LEFT JOIN hc_precadastros p ON p.ixc_vendedor_id = v.id
            AND p.status = 'ativado'
            AND strftime('%Y-%m', p.criado_em) = ?
        WHERE v.ativo = 1
        GROUP BY v.id
        ORDER BY realizados DESC, v.nome
    """, (mes, mes)).fetchall()

    return {
        "mes": mes,
        "vendedores": [dict(r) for r in rows]
    }


class MetaPayload(BaseModel):
    vendedor_id: int
    mes: str
    meta: int


@router.post("")
async def salvar_meta(
    payload: MetaPayload,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    if payload.meta < 0:
        raise HTTPException(400, "Meta não pode ser negativa.")

    db.execute("""
        INSERT INTO hc_metas(vendedor_id, mes, meta)
        VALUES(?,?,?)
        ON CONFLICT(vendedor_id, mes) DO UPDATE SET meta=excluded.meta
    """, (payload.vendedor_id, payload.mes, payload.meta))
    db.commit()
    return {"ok": True, "msg": f"Meta de {payload.meta} ativações salva para {payload.mes}"}


@router.get("/historico/{vendedor_id}")
async def historico_metas(
    vendedor_id: int,
    db=Depends(get_db),
    user=Depends(requer_vendedor())
):
    rows = db.execute("""
        SELECT m.mes, m.meta, v.nome as vendedor_nome,
               COUNT(p.id) as realizados
        FROM hc_metas m
        JOIN hc_vendedores v ON v.id = m.vendedor_id
        LEFT JOIN hc_precadastros p ON p.ixc_vendedor_id = m.vendedor_id
            AND p.status = 'ativado'
            AND strftime('%Y-%m', p.criado_em) = m.mes
        WHERE m.vendedor_id = ?
        GROUP BY m.mes
        ORDER BY m.mes DESC
        LIMIT 12
    """, (vendedor_id,)).fetchall()
    return {"historico": [dict(r) for r in rows]}
