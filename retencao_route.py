"""
app/routes/retencao.py
Retenção por vencimento — Hub Comercial CliqueDF

Registrar em main.py:
    from app.routes.retencao import router as retencao_router
    app.include_router(retencao_router)
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import sqlite3

from app.services.ixc_db import ixc_conn
from app.services.auth import requer_login

router = APIRouter(prefix="/api/painel/retencao", tags=["retencao"])

DB = "hub_comercial.db"

def get_db():
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# ─────────────────────────────────────────────
# GET /api/painel/retencao?mes=2026-07
# ─────────────────────────────────────────────
@router.get("")
def listar_retencao(
    mes: str = Query(..., description="Mês no formato YYYY-MM, ex: 2026-07"),
    cidade: Optional[str] = Query(None),
    plano: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    usuario=Depends(requer_login),
    db: sqlite3.Connection = Depends(get_db),
):
    """
    Busca contratos que vencem no mês informado direto do IXC,
    cruza com hc_retencao para trazer status de atendimento.
    """
    try:
        ano, mes_num = mes.split("-")
        mes_inicio = f"{ano}-{mes_num}-01"
        # último dia do mês
        import calendar
        ultimo_dia = calendar.monthrange(int(ano), int(mes_num))[1]
        mes_fim = f"{ano}-{mes_num}-{ultimo_dia:02d}"
    except Exception:
        raise HTTPException(status_code=400, detail="Formato de mês inválido. Use YYYY-MM")

    # Busca no IXC
    sql_ixc = """
        SELECT
            cc.id                        AS contrato_id,
            c.id                         AS cliente_id,
            c.razao                      AS cliente,
            cc.contrato                  AS plano_nome,
            vd.valor_contrato            AS plano_valor,
            cc.data_expiracao            AS data_expiracao,
            c.fone_celular               AS telefone,
            cid.nome                     AS cidade_nome
        FROM cliente_contrato cc
        INNER JOIN cliente c         ON c.id   = cc.id_cliente
        INNER JOIN vd_contratos vd   ON vd.id  = cc.id_vd_contrato
        LEFT  JOIN cidades cid       ON cid.id = cc.cidade
        WHERE cc.status = 'A'
          AND cc.data_expiracao >= %s
          AND cc.data_expiracao <= %s
        ORDER BY cc.data_expiracao ASC, c.razao ASC
    """

    with ixc_conn() as ixc:
        with ixc.cursor() as cur:
            cur.execute(sql_ixc, (mes_inicio, mes_fim))
            contratos_ixc = cur.fetchall()

    # Busca status já registrados no hub
    ids = [r["contrato_id"] for r in contratos_ixc]
    status_hub = {}
    if ids:
        placeholders = ",".join("?" * len(ids))
        rows_hub = db.execute(
            f"SELECT ixc_contrato_id, status_retencao, obs, responsavel FROM hc_retencao WHERE ixc_contrato_id IN ({placeholders})",
            ids,
        ).fetchall()
        for r in rows_hub:
            status_hub[r["ixc_contrato_id"]] = dict(r)

    # Monta resultado
    resultado = []
    for r in contratos_ixc:
        cid = r["contrato_id"]
        hub = status_hub.get(cid, {})

        item = {
            "contrato_id":    cid,
            "cliente_id":     r["cliente_id"],
            "cliente":        r["cliente"],
            "plano_nome":     r["plano_nome"],
            "plano_valor":    float(r["plano_valor"] or 0),
            "data_expiracao": str(r["data_expiracao"]),
            "telefone":       r["telefone"] or "",
            "cidade_nome":    r["cidade_nome"] or "",
            "status_retencao": hub.get("status_retencao", "pendente"),
            "obs":            hub.get("obs", ""),
            "responsavel":    hub.get("responsavel", ""),
        }

        # Filtros opcionais
        if cidade and cidade.lower() not in (item["cidade_nome"] or "").lower():
            continue
        if plano and plano.lower() not in (item["plano_nome"] or "").lower():
            continue
        if status and item["status_retencao"] != status:
            continue

        resultado.append(item)

    # KPIs
    total       = len(resultado)
    receita     = sum(i["plano_valor"] for i in resultado)
    retidos     = sum(1 for i in resultado if i["status_retencao"] == "retido")
    cancelados  = sum(1 for i in resultado if i["status_retencao"] == "cancelado")
    em_contato  = sum(1 for i in resultado if i["status_retencao"] == "em_contato")
    pendentes   = sum(1 for i in resultado if i["status_retencao"] == "pendente")

    return {
        "mes":       mes,
        "kpis": {
            "total":      total,
            "receita":    round(receita, 2),
            "retidos":    retidos,
            "cancelados": cancelados,
            "em_contato": em_contato,
            "pendentes":  pendentes,
        },
        "contratos": resultado,
    }


# ─────────────────────────────────────────────
# GET /api/painel/retencao/meses
# Retorna os meses disponíveis com totais
# ─────────────────────────────────────────────
@router.get("/meses")
def meses_disponiveis(usuario=Depends(requer_login)):
    MESES_PT = {
        "01": "Janeiro",  "02": "Fevereiro", "03": "Março",
        "04": "Abril",    "05": "Maio",       "06": "Junho",
        "07": "Julho",    "08": "Agosto",     "09": "Setembro",
        "10": "Outubro",  "11": "Novembro",   "12": "Dezembro",
    }
    sql = """
        SELECT
            DATE_FORMAT(cc.data_expiracao, '%Y-%m') AS mes_chave,
            COUNT(*)                                 AS total,
            SUM(vd.valor_contrato)                   AS receita
        FROM cliente_contrato cc
        INNER JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
        WHERE cc.status = 'A'
          AND cc.data_expiracao >= CURDATE()
          AND cc.data_expiracao IS NOT NULL
          AND cc.data_expiracao != '0000-00-00'
        GROUP BY mes_chave
        ORDER BY mes_chave ASC
    """
    with ixc_conn() as ixc:
        with ixc.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    resultado = []
    for r in rows:
        ano, mes_num = r["mes_chave"].split("-")
        resultado.append({
            "mes":       r["mes_chave"],
            "label":     f"{MESES_PT[mes_num]}/{ano}",
            "total":     r["total"],
            "receita":   round(float(r["receita"] or 0), 2),
        })
    return resultado


# ─────────────────────────────────────────────
# POST /api/painel/retencao/{contrato_id}/status
# Salva ou atualiza status de atendimento
# ─────────────────────────────────────────────
class StatusPayload(BaseModel):
    status_retencao: str   # pendente | em_contato | retido | cancelado
    obs: Optional[str] = ""
    cliente: Optional[str] = ""
    ixc_cliente_id: Optional[int] = None
    plano_nome: Optional[str] = ""
    plano_valor: Optional[float] = 0
    data_expiracao: Optional[str] = ""
    telefone: Optional[str] = ""
    cidade_nome: Optional[str] = ""

STATUSES_VALIDOS = {"pendente", "em_contato", "retido", "cancelado"}

@router.post("/{contrato_id}/status")
def atualizar_status(
    contrato_id: int,
    payload: StatusPayload,
    usuario=Depends(requer_login),
    db: sqlite3.Connection = Depends(get_db),
):
    if payload.status_retencao not in STATUSES_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Status inválido: {payload.status_retencao}")

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    responsavel = usuario.get("nome", "")

    existe = db.execute(
        "SELECT id FROM hc_retencao WHERE ixc_contrato_id = ?", (contrato_id,)
    ).fetchone()

    if existe:
        db.execute("""
            UPDATE hc_retencao SET
                status_retencao = ?,
                obs             = ?,
                responsavel     = ?,
                atualizado_em   = ?
            WHERE ixc_contrato_id = ?
        """, (payload.status_retencao, payload.obs, responsavel, agora, contrato_id))
    else:
        db.execute("""
            INSERT INTO hc_retencao
                (ixc_contrato_id, ixc_cliente_id, cliente, plano_nome, plano_valor,
                 data_expiracao, telefone, cidade_nome,
                 status_retencao, obs, responsavel, criado_em, atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            contrato_id,
            payload.ixc_cliente_id,
            payload.cliente,
            payload.plano_nome,
            payload.plano_valor,
            payload.data_expiracao,
            payload.telefone,
            payload.cidade_nome,
            payload.status_retencao,
            payload.obs,
            responsavel,
            agora, agora,
        ))
    db.commit()

    return {"ok": True, "contrato_id": contrato_id, "status": payload.status_retencao}
