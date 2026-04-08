"""
Hub Comercial — app/routes/upgrade.py
=======================================
Endpoints do menu Upgrade.

GET  /api/upgrade/base           → lista clientes para upgrade (com filtros)
GET  /api/upgrade/base/{id}      → detalhe de um cliente
POST /api/upgrade/base/{id}/negociacao → atualiza status de negociação
GET  /api/upgrade/planos         → planos disponíveis no IXC para seleção
POST /api/upgrade/realizar       → registra e aplica o upgrade no IXC
GET  /api/upgrade/log            → histórico de upgrades realizados
GET  /api/upgrade/resumo         → KPIs: receita antes/depois, total upgrades
"""
import sqlite3, logging, os
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from app.services.auth import requer_backoffice, requer_supervisor
from app.services.ixc_db import ixc_select, ixc_select_one, ixc_conn

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "hub_comercial.db"
log      = logging.getLogger(__name__)
router   = APIRouter()


def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    try: yield c
    finally: c.close()


def agora():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Lista de clientes para upgrade ───────────────────────────

@router.get("/base")
async def listar_base(
    cidade:   str = Query(""),
    plano:    str = Query(""),
    status:   str = Query("todos"),   # todos | nao_contatado | em_contato | negociando | confirmado | recusou
    busca:    str = Query(""),
    pagina:   int = Query(1, ge=1),
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """
    Lista clientes da base de upgrade com filtros.
    Retorna paginado (50 por página).
    """
    pp = 50
    offset = (pagina - 1) * pp
    where, params = ["1=1"], []

    if cidade:
        where.append("cidade = ?"); params.append(cidade)
    if plano:
        where.append("plano_nome LIKE ?"); params.append(f"%{plano}%")
    if status != "todos":
        where.append("status_negociacao = ?"); params.append(status)
    if busca:
        where.append("(cliente LIKE ? OR telefone_celular LIKE ? OR ixc_contrato_id LIKE ?)")
        params += [f"%{busca}%", f"%{busca}%", f"%{busca}%"]

    w = " AND ".join(where)
    total = db.execute(f"SELECT COUNT(*) FROM hc_upgrades_base WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"""
        SELECT * FROM hc_upgrades_base
        WHERE {w}
        ORDER BY cidade, plano_nome, cliente
        LIMIT ? OFFSET ?
    """, params + [pp, offset]).fetchall()

    return {
        "total":   total,
        "pagina":  pagina,
        "por_pagina": pp,
        "clientes": [dict(r) for r in rows]
    }


# ── Detalhe de um cliente ─────────────────────────────────────

@router.get("/base/{id}")
async def detalhe_base(id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    """Retorna os dados completos de um cliente da base de upgrade."""
    row = db.execute("SELECT * FROM hc_upgrades_base WHERE id=?", (id,)).fetchone()
    if not row: raise HTTPException(404, "Não encontrado.")

    # Histórico de upgrades já feitos para esse contrato
    logs = db.execute("""
        SELECT * FROM hc_upgrades_log
        WHERE ixc_contrato_id=?
        ORDER BY realizado_em DESC
    """, (row["ixc_contrato_id"],)).fetchall()

    return {
        "cliente": dict(row),
        "historico": [dict(l) for l in logs]
    }


# ── Atualizar status de negociação ────────────────────────────

class NegociacaoPayload(BaseModel):
    status_negociacao: str   # nao_contatado | em_contato | negociando | confirmado | recusou
    obs_negociacao:    Optional[str] = ""

@router.post("/base/{id}/negociacao")
async def atualizar_negociacao(
    id: int,
    payload: NegociacaoPayload,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """Atualiza o status de negociação de um cliente."""
    status_validos = {"nao_contatado", "em_contato", "negociando", "confirmado", "recusou"}
    if payload.status_negociacao not in status_validos:
        raise HTTPException(400, f"Status inválido. Use: {status_validos}")

    row = db.execute("SELECT id FROM hc_upgrades_base WHERE id=?", (id,)).fetchone()
    if not row: raise HTTPException(404, "Não encontrado.")

    db.execute("""
        UPDATE hc_upgrades_base
        SET status_negociacao=?, obs_negociacao=?
        WHERE id=?
    """, (payload.status_negociacao, payload.obs_negociacao, id))
    db.commit()
    return {"ok": True}


# ── Planos disponíveis no IXC ─────────────────────────────────

@router.get("/planos")
async def listar_planos_ixc(user=Depends(requer_backoffice())):
    """
    Busca planos ativos do IXC para seleção no modal de upgrade.
    Retorna id, nome e valor de cada plano.
    """
    try:
        rows = ixc_select("""
            SELECT id, nome, valor_contrato AS valor
            FROM ixcprovedor.vd_contratos
            WHERE Ativo = 'S'
            ORDER BY nome
        """)
        def sv(v):
            if v is None: return None
            if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal': return float(v)
            return v
        return {"planos": [{k: sv(val) for k, val in dict(r).items()} for r in rows]}
    except Exception as e:
        log.error(f"listar_planos_ixc: {e}")
        return {"planos": []}


# ── Realizar upgrade ──────────────────────────────────────────

class UpgradePayload(BaseModel):
    base_id:          int            # ID da hc_upgrades_base
    plano_novo_id:    int            # ID do novo plano no IXC
    plano_novo_nome:  str
    plano_novo_valor: float
    obs:              Optional[str] = ""

@router.post("/realizar")
async def realizar_upgrade(
    payload: UpgradePayload,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """
    Registra o upgrade no log e atualiza o id_vd_contrato no IXC.

    O UPDATE no IXC é feito via MySQL direto na tabela cliente_contrato.
    """
    # Buscar dados do cliente
    row = db.execute(
        "SELECT * FROM hc_upgrades_base WHERE id=?", (payload.base_id,)
    ).fetchone()
    if not row: raise HTTPException(404, "Cliente não encontrado.")

    p = dict(row)
    plano_ant_id    = p["ixc_plano_id"]
    plano_ant_nome  = p["plano_nome"]
    plano_ant_valor = None

    # Buscar valor do plano anterior no IXC
    try:
        plano_ant = ixc_select_one(
            "SELECT valor_contrato FROM ixcprovedor.vd_contratos WHERE id=%s",
            (plano_ant_id,)
        )
        if plano_ant:
            v = plano_ant["valor_contrato"]
            plano_ant_valor = float(v) if v else 0.0
    except Exception as e:
        log.warning(f"Erro ao buscar valor plano anterior: {e}")

    diferenca = round((payload.plano_novo_valor or 0) - (plano_ant_valor or 0), 2)
    if diferenca > 0:
        tipo_mudanca = "upgrade"
    elif diferenca < 0:
        tipo_mudanca = "downgrade"
    else:
        tipo_mudanca = "lateral"

    # Registrar no log
    db.execute("""
        INSERT INTO hc_upgrades_log (
            ixc_contrato_id, cliente, cidade,
            plano_anterior_id, plano_anterior_nome, plano_anterior_valor,
            plano_novo_id, plano_novo_nome, plano_novo_valor,
            diferenca_valor, tipo_mudanca,
            obs, realizado_por, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        p["ixc_contrato_id"], p["cliente"], p["cidade"],
        plano_ant_id, plano_ant_nome, plano_ant_valor,
        payload.plano_novo_id, payload.plano_novo_nome, payload.plano_novo_valor,
        diferenca, tipo_mudanca,
        payload.obs, user["login"], "pendente"
    ))
    log_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.commit()

    # Aplicar no IXC via MySQL direto
    try:
        with ixc_conn() as ixc:
            with ixc.cursor() as cur:
                cur.execute("""
                    UPDATE ixcprovedor.cliente_contrato
                    SET id_vd_contrato = %s,
                        contrato = %s,
                        descricao_aux_plano_venda = %s,
                        valor_unitario = %s,
                        ultima_atualizacao = NOW()
                    WHERE id = %s
                """, (
                    payload.plano_novo_id,
                    payload.plano_novo_nome,
                    payload.plano_novo_nome,
                    payload.plano_novo_valor,
                    p["ixc_contrato_id"]
                ))
                ixc.commit()

        # Marcar como aplicado no log e atualizar base
        db.execute(
            "UPDATE hc_upgrades_log SET status='aplicado' WHERE id=?", (log_id,)
        )
        # Atualizar plano na base local
        db.execute("""
            UPDATE hc_upgrades_base
            SET ixc_plano_id=?, plano_nome=?, status_negociacao='confirmado'
            WHERE id=?
        """, (payload.plano_novo_id, payload.plano_novo_nome, payload.base_id))
        db.commit()

        log.info(
            f"Upgrade #{p['ixc_contrato_id']} — {p['cliente']} — "
            f"{plano_ant_nome} → {payload.plano_novo_nome} — por {user['login']}"
        )
        return {
            "ok": True,
            "tipo": tipo_mudanca,
            "diferenca": diferenca,
            "msg": f"Contrato #{p['ixc_contrato_id']} atualizado para {payload.plano_novo_nome}"
        }

    except Exception as e:
        db.execute(
            "UPDATE hc_upgrades_log SET status='erro' WHERE id=?", (log_id,)
        )
        db.commit()
        log.error(f"Erro ao aplicar upgrade #{p['ixc_contrato_id']}: {e}")
        raise HTTPException(500, f"Erro ao atualizar contrato no IXC: {e}")


# ── Histórico de upgrades ─────────────────────────────────────

@router.get("/log")
async def historico_upgrades(
    tipo:   str = Query("todos"),   # todos | upgrade | downgrade | lateral
    status: str = Query("todos"),   # todos | pendente | aplicado | erro
    cidade: str = Query(""),
    pagina: int = Query(1, ge=1),
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """Histórico paginado de todos os upgrades realizados."""
    pp = 50
    offset = (pagina - 1) * pp
    where, params = ["1=1"], []

    if tipo != "todos":
        where.append("tipo_mudanca=?"); params.append(tipo)
    if status != "todos":
        where.append("status=?"); params.append(status)
    if cidade:
        where.append("cidade=?"); params.append(cidade)

    w = " AND ".join(where)
    total = db.execute(f"SELECT COUNT(*) FROM hc_upgrades_log WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"""
        SELECT * FROM hc_upgrades_log WHERE {w}
        ORDER BY realizado_em DESC
        LIMIT ? OFFSET ?
    """, params + [pp, offset]).fetchall()

    return {
        "total": total,
        "pagina": pagina,
        "log": [dict(r) for r in rows]
    }


# ── KPIs / Resumo ─────────────────────────────────────────────

@router.get("/resumo")
async def resumo_upgrades(db=Depends(get_db), user=Depends(requer_backoffice())):
    """
    KPIs do menu upgrade:
    - Total de clientes na base
    - Por status de negociação
    - Upgrades realizados (total, receita antes, receita depois, ganho/perda)
    """
    # Base
    base = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(status_negociacao='nao_contatado') AS nao_contatado,
            SUM(status_negociacao='em_contato')    AS em_contato,
            SUM(status_negociacao='negociando')    AS negociando,
            SUM(status_negociacao='confirmado')    AS confirmado,
            SUM(status_negociacao='recusou')       AS recusou
        FROM hc_upgrades_base
    """).fetchone()

    # Log
    log_res = db.execute("""
        SELECT
            COUNT(*) AS total_upgrades,
            SUM(tipo_mudanca='upgrade')   AS upgrades,
            SUM(tipo_mudanca='downgrade') AS downgrades,
            SUM(tipo_mudanca='lateral')   AS laterais,
            SUM(plano_anterior_valor)     AS receita_antes,
            SUM(plano_novo_valor)         AS receita_depois,
            SUM(diferenca_valor)          AS ganho_total
        FROM hc_upgrades_log
        WHERE status='aplicado'
    """).fetchone()

    # Planos mais comuns na base
    planos = db.execute("""
        SELECT plano_nome, COUNT(*) AS qtd
        FROM hc_upgrades_base
        GROUP BY plano_nome
        ORDER BY qtd DESC
        LIMIT 10
    """).fetchall()

    # Cidades
    cidades = db.execute("""
        SELECT cidade, COUNT(*) AS qtd
        FROM hc_upgrades_base
        GROUP BY cidade ORDER BY qtd DESC
    """).fetchall()

    def safe(v): return v if v is not None else 0

    return {
        "base": {
            "total":         safe(base["total"]),
            "nao_contatado": safe(base["nao_contatado"]),
            "em_contato":    safe(base["em_contato"]),
            "negociando":    safe(base["negociando"]),
            "confirmado":    safe(base["confirmado"]),
            "recusou":       safe(base["recusou"]),
        },
        "log": {
            "total":          safe(log_res["total_upgrades"]),
            "upgrades":       safe(log_res["upgrades"]),
            "downgrades":     safe(log_res["downgrades"]),
            "laterais":       safe(log_res["laterais"]),
            "receita_antes":  round(safe(log_res["receita_antes"]), 2),
            "receita_depois": round(safe(log_res["receita_depois"]), 2),
            "ganho_total":    round(safe(log_res["ganho_total"]), 2),
        },
        "planos": [dict(r) for r in planos],
        "cidades": [dict(r) for r in cidades],
    }
