from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import sqlite3, calendar

from app.services.ixc_db import ixc_conn
from app.services.auth import requer_backoffice

router = APIRouter(prefix="/api/painel/retencao", tags=["retencao"])
DB = "hub_comercial.db"

def get_db():
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

@router.get("")
def listar_retencao(
    mes: str = Query(...),
    cidade: Optional[str] = Query(None),
    plano: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        ano, mes_num = mes.split("-")
        mes_inicio = f"{ano}-{mes_num}-01"
        ultimo_dia = calendar.monthrange(int(ano), int(mes_num))[1]
        mes_fim = f"{ano}-{mes_num}-{ultimo_dia:02d}"
    except Exception:
        raise HTTPException(status_code=400, detail="Formato inválido. Use YYYY-MM")

    sql_ixc = """
        SELECT cc.id AS contrato_id, c.id AS cliente_id, c.razao AS cliente,
               cc.contrato AS plano_nome, vd.valor_contrato AS plano_valor,
               cc.data_expiracao, c.telefone_celular AS telefone,
               ci.nome AS cidade_nome
        FROM cliente_contrato cc
        INNER JOIN cliente c          ON c.id  = cc.id_cliente
        INNER JOIN vd_contratos vd    ON vd.id = cc.id_vd_contrato
        LEFT  JOIN cidade ci          ON ci.id = c.cidade
        WHERE cc.status = 'A'
          AND cc.data_expiracao >= %s
          AND cc.data_expiracao <= %s
        ORDER BY cc.data_expiracao ASC, c.razao ASC
    """
    with ixc_conn() as ixc:
        with ixc.cursor() as cur:
            cur.execute(sql_ixc, (mes_inicio, mes_fim))
            contratos_ixc = cur.fetchall()

    ids = [r["contrato_id"] for r in contratos_ixc]
    status_hub = {}
    if ids:
        ph = ",".join("?" * len(ids))
        rows_hub = db.execute(
            f"SELECT ixc_contrato_id, status_retencao, obs, responsavel FROM hc_retencao WHERE ixc_contrato_id IN ({ph})", ids
        ).fetchall()
        for r in rows_hub:
            status_hub[r["ixc_contrato_id"]] = dict(r)

    resultado = []
    for r in contratos_ixc:
        cid = r["contrato_id"]
        hub = status_hub.get(cid, {})
        item = {
            "contrato_id": cid, "cliente_id": r["cliente_id"],
            "cliente": r["cliente"], "plano_nome": r["plano_nome"],
            "plano_valor": float(r["plano_valor"] or 0),
            "data_expiracao": str(r["data_expiracao"]),
            "telefone": r["telefone"] or "", "cidade_nome": r["cidade_nome"] or "",
            "status_retencao": hub.get("status_retencao", "pendente"),
            "obs": hub.get("obs", ""), "responsavel": hub.get("responsavel", ""),
        }
        if cidade and cidade.lower() not in (item["cidade_nome"]).lower(): continue
        if plano  and plano.lower()  not in (item["plano_nome"] or "").lower(): continue
        if status and item["status_retencao"] != status: continue
        resultado.append(item)

    total = len(resultado)
    return {
        "mes": mes,
        "kpis": {
            "total": total,
            "receita":    round(sum(i["plano_valor"] for i in resultado), 2),
            "retidos":    sum(1 for i in resultado if i["status_retencao"] == "retido"),
            "cancelados": sum(1 for i in resultado if i["status_retencao"] == "cancelado"),
            "em_contato": sum(1 for i in resultado if i["status_retencao"] == "em_contato"),
            "pendentes":  sum(1 for i in resultado if i["status_retencao"] == "pendente"),
        },
        "contratos": resultado,
    }

@router.get("/meses")
def meses_disponiveis(usuario=Depends(requer_backoffice())):
    MESES_PT = {"01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril","05":"Maio",
                "06":"Junho","07":"Julho","08":"Agosto","09":"Setembro",
                "10":"Outubro","11":"Novembro","12":"Dezembro"}
    sql = """
        SELECT DATE_FORMAT(cc.data_expiracao,'%Y-%m') AS mes_chave,
               COUNT(*) AS total, SUM(vd.valor_contrato) AS receita
        FROM cliente_contrato cc
        INNER JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
        WHERE cc.status='A' AND cc.data_expiracao >= CURDATE()
          AND cc.data_expiracao IS NOT NULL AND cc.data_expiracao != '0000-00-00'
        GROUP BY mes_chave ORDER BY mes_chave ASC
    """
    with ixc_conn() as ixc:
        with ixc.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    resultado = []
    for r in rows:
        ano, mes_num = r["mes_chave"].split("-")
        resultado.append({"mes": r["mes_chave"], "label": f"{MESES_PT[mes_num]}/{ano}",
                          "total": r["total"], "receita": round(float(r["receita"] or 0), 2)})
    return resultado

class StatusPayload(BaseModel):
    status_retencao: str
    obs: Optional[str] = ""
    cliente: Optional[str] = ""
    ixc_cliente_id: Optional[int] = None
    plano_nome: Optional[str] = ""
    plano_valor: Optional[float] = 0
    data_expiracao: Optional[str] = ""
    telefone: Optional[str] = ""
    cidade_nome: Optional[str] = ""

@router.post("/{contrato_id}/status")
def atualizar_status(
    contrato_id: int, payload: StatusPayload,
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    VALIDOS = {"pendente","em_contato","retido","cancelado"}
    if payload.status_retencao not in VALIDOS:
        raise HTTPException(status_code=400, detail=f"Status inválido: {payload.status_retencao}")
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    responsavel = usuario.get("nome","")
    existe = db.execute("SELECT id FROM hc_retencao WHERE ixc_contrato_id=?", (contrato_id,)).fetchone()
    if existe:
        db.execute("UPDATE hc_retencao SET status_retencao=?,obs=?,responsavel=?,atualizado_em=? WHERE ixc_contrato_id=?",
                   (payload.status_retencao, payload.obs, responsavel, agora, contrato_id))
    else:
        db.execute("""INSERT INTO hc_retencao
            (ixc_contrato_id,ixc_cliente_id,cliente,plano_nome,plano_valor,
             data_expiracao,telefone,cidade_nome,status_retencao,obs,responsavel,criado_em,atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (contrato_id, payload.ixc_cliente_id, payload.cliente, payload.plano_nome,
             payload.plano_valor, payload.data_expiracao, payload.telefone, payload.cidade_nome,
             payload.status_retencao, payload.obs, responsavel, agora, agora))
    db.commit()
    return {"ok": True, "contrato_id": contrato_id, "status": payload.status_retencao}

# ─────────────────────────────────────────────────────────────
# ALTERAÇÃO DE PLANO — reaproveitando motor de upgrade
# ─────────────────────────────────────────────────────────────
import os, logging
log = logging.getLogger(__name__)

class AlteracaoPlanoPayload(BaseModel):
    ixc_contrato_id:  int
    cliente:          str
    cidade_nome:      str
    plano_atual_id:   int
    plano_atual_nome: str
    plano_atual_valor: float
    plano_novo_id:    int
    plano_novo_nome:  str
    plano_novo_valor: float
    status_contato:   str   # nao_contatado | em_contato | negociando | confirmado | recusou
    obs:              Optional[str] = ""
    apenas_registrar: bool = False  # True=só registra, False=aplica no IXC

@router.post("/alterar-plano")
def alterar_plano(
    payload: AlteracaoPlanoPayload,
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    responsavel = usuario.get("nome", "") or usuario.get("login", "")
    diferenca   = round(payload.plano_novo_valor - payload.plano_atual_valor, 2)
    tipo_mudanca = "upgrade" if diferenca > 0 else "downgrade" if diferenca < 0 else "lateral"

    # 1. Salva/atualiza status na hc_retencao
    existe = db.execute(
        "SELECT id FROM hc_retencao WHERE ixc_contrato_id=?", (payload.ixc_contrato_id,)
    ).fetchone()
    if existe:
        db.execute("""
            UPDATE hc_retencao SET
                status_retencao=?, obs=?, responsavel=?, atualizado_em=?
            WHERE ixc_contrato_id=?
        """, (payload.status_contato, payload.obs, responsavel, agora, payload.ixc_contrato_id))
    else:
        db.execute("""
            INSERT INTO hc_retencao
                (ixc_contrato_id, ixc_cliente_id, cliente, plano_nome, plano_valor,
                 cidade_nome, status_retencao, obs, responsavel, criado_em, atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            payload.ixc_contrato_id, None, payload.cliente,
            payload.plano_atual_nome, payload.plano_atual_valor,
            payload.cidade_nome, payload.status_contato,
            payload.obs, responsavel, agora, agora,
        ))

    # 2. Notifica Telegram para qualquer interação
    _notif_alteracao(payload, diferenca, tipo_mudanca, responsavel)

    # 3. Se apenas_registrar, para aqui
    if payload.apenas_registrar or payload.status_contato not in ("confirmado",):
        db.commit()
        return {"ok": True, "tipo": tipo_mudanca, "diferenca": diferenca,
                "msg": f"Status registrado: {payload.status_contato}"}

    # 4. Aplica no IXC via MySQL
    try:
        from app.services.ixc_db import ixc_conn as _ixc_conn
        with _ixc_conn() as ixc:
            with ixc.cursor() as cur:
                cur.execute("""
                    UPDATE cliente_contrato
                    SET id_vd_contrato            = %s,
                        contrato                  = %s,
                        descricao_aux_plano_venda = %s,
                        valor_unitario            = %s,
                        ultima_atualizacao        = NOW()
                    WHERE id = %s
                """, (
                    payload.plano_novo_id,
                    payload.plano_novo_nome,
                    payload.plano_novo_nome,
                    payload.plano_novo_valor,
                    payload.ixc_contrato_id,
                ))
                ixc.commit()

        # Atualiza status para alterado
        db.execute("""
            UPDATE hc_retencao SET status_retencao='alterado', atualizado_em=?
            WHERE ixc_contrato_id=?
        """, (agora, payload.ixc_contrato_id))
        db.commit()

        _notif_alteracao_aplicada(payload, diferenca, tipo_mudanca, responsavel)

        return {"ok": True, "tipo": tipo_mudanca, "diferenca": diferenca,
                "msg": f"Plano alterado para {payload.plano_novo_nome} no IXC!"}

    except Exception as e:
        db.commit()
        log.error(f"Erro ao alterar plano #{payload.ixc_contrato_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar no IXC: {e}")


def _notif_alteracao(payload, diferenca, tipo, por):
    """Notifica toda interação — contato, negociação, recusa."""
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id: return

    icons = {
        "nao_contatado": "📋", "em_contato": "📞",
        "negociando": "🤝",   "confirmado": "✅", "recusou": "❌",
    }
    labels = {
        "nao_contatado": "NÃO CONTATADO", "em_contato": "EM CONTATO",
        "negociando": "NEGOCIANDO",       "confirmado": "CONFIRMADO", "recusou": "RECUSOU",
    }
    icon  = icons.get(payload.status_contato, "📋")
    label = labels.get(payload.status_contato, payload.status_contato.upper())

    msg = (
        f"{icon} *VENCIMENTO — {label}*\n\n"
        f"👤 *Cliente:* {payload.cliente}\n"
        f"📍 *Cidade:* {payload.cidade_nome}\n"
        f"📋 *Contrato:* #{payload.ixc_contrato_id}\n"
        f"📦 *Plano atual:* {payload.plano_atual_nome} — R$ {payload.plano_atual_valor:.2f}\n"
    )
    if payload.status_contato == "confirmado":
        sinal = "+" if diferenca >= 0 else ""
        msg += (
            f"🚀 *Novo plano:* {payload.plano_novo_nome} — R$ {payload.plano_novo_valor:.2f}\n"
            f"💰 *Diferença:* {sinal}R$ {diferenca:.2f}/mês\n"
        )
    if payload.obs:
        msg += f"💬 *Obs:* {payload.obs}\n"
    msg += f"\n👨‍💼 *Por:* {por}"

    try:
        _req.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Telegram alteracao: {e}")


def _notif_alteracao_aplicada(payload, diferenca, tipo, por):
    """Notifica quando o plano é efetivamente aplicado no IXC."""
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id: return

    icon = "⬆️" if tipo == "upgrade" else "⬇️" if tipo == "downgrade" else "↔️"
    sinal = "+" if diferenca >= 0 else ""
    msg = (
        f"{icon} *PLANO ALTERADO — INTEGRADO IXC* ✅\n\n"
        f"👤 *Cliente:* {payload.cliente}\n"
        f"📍 *Cidade:* {payload.cidade_nome}\n"
        f"📋 *Contrato:* #{payload.ixc_contrato_id}\n\n"
        f"📦 *De:* {payload.plano_atual_nome}\n"
        f"🚀 *Para:* {payload.plano_novo_nome}\n"
        f"💰 *Diferença:* {sinal}R$ {diferenca:.2f}/mês\n\n"
        f"👨‍💼 *Por:* {por}"
    )
    try:
        _req.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Telegram alteracao aplicada: {e}")
