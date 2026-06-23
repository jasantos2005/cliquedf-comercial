"""
app/routes/alteracao_planos.py
Alteração de Planos por Vencimento — Hub Comercial CliqueDF
Prefixo: /api/alteracao-planos
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import sqlite3, calendar, os, logging

from app.services.ixc_db import ixc_conn
from app.services.auth import requer_backoffice

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/alteracao-planos", tags=["alteracao-planos"])
DB = "hub_comercial.db"

# De-para fixo de planos
DEPARA_PLANOS = {
    161: 167,   # 150MB R$55 → 150MB R$60
    167: 166,   # 150MB R$60 → 200MB R$65
    166: 162,   # 200MB R$65 → 300MB R$69,90
    162: 171,   # 300MB R$69,90 → 400MB R$74,90
    157: 171,   # Retencao 200MB R$69,90 → 400MB R$74,90
    171: 175,   # 400MB R$74,90 → 500MB R$79,90
    145: 175,   # 200MB+TV R$79 → 500MB R$79,90
    175: 163,   # 500MB R$79,90 → 600MB R$89,90
    126: 163,   # 300MB+32 Canais R$89 → 600MB R$89,90
    163: 164,   # 600MB R$89,90 → 800MB R$99,90
    164: 172,   # 800MB R$99,90 → 900MB Plus R$105,90
    151: 172,   # 500MB+TV R$95 → 800MB Plus R$102,90
    146: 172,   # 500MB Premium R$97 → 800MB Plus R$102,90
    172: 173,   # 800MB Plus R$105,90 → 1G Plus R$122,90
    165: 173,   # 1G R$119,90 → 1G Plus R$122,90
    147: 173,   # 700MB+TV R$115 → 1G Plus R$122,90
    173: 174,   # 1G Plus R$122,90 → 1G Premium R$154
    148: 174,   # TURBO 1GB+TV R$147 → 1G Premium R$154
    140: 168,   # PME 500MB R$350 → PME 800MB R$400
}

def get_db():
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


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


@router.get("")
def listar_por_mes(
    mes: str = Query(...),
    cidade: Optional[str] = Query(None),
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
               cc.id_vd_contrato AS plano_id,
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

    # Busca plano substituto para cada contrato
    ids_planos = list(set([r["plano_id"] for r in contratos_ixc if r["plano_id"]]))
    planos_novos = {}
    if ids_planos:
        ids_depara = [pid for pid in ids_planos if pid in DEPARA_PLANOS]
        if ids_depara:
            novos_ids = list(set([DEPARA_PLANOS[pid] for pid in ids_depara]))
            ph = ",".join(["%s"] * len(novos_ids))
            with ixc_conn() as ixc:
                with ixc.cursor() as cur:
                    cur.execute(f"SELECT id, nome, valor_contrato FROM vd_contratos WHERE id IN ({ph})", novos_ids)
                    for r in cur.fetchall():
                        planos_novos[r["id"]] = {
                            "id": r["id"],
                            "nome": r["nome"],
                            "valor": float(r["valor_contrato"] or 0),
                        }

    # Status do hub
    ids = [r["contrato_id"] for r in contratos_ixc]
    status_hub = {}
    if ids:
        ph = ",".join("?" * len(ids))
        rows_hub = db.execute(
            f"SELECT ixc_contrato_id, status_alteracao, obs, responsavel FROM hc_alteracao_planos WHERE ixc_contrato_id IN ({ph})", ids
        ).fetchall()
        for r in rows_hub:
            status_hub[r["ixc_contrato_id"]] = dict(r)

    resultado = []
    for r in contratos_ixc:
        cid = r["contrato_id"]
        hub = status_hub.get(cid, {})
        plano_id = r["plano_id"]
        novo_plano = None
        if plano_id in DEPARA_PLANOS:
            novo_id = DEPARA_PLANOS[plano_id]
            novo_plano = planos_novos.get(novo_id)

        item = {
            "contrato_id":    cid,
            "cliente_id":     r["cliente_id"],
            "cliente":        r["cliente"],
            "plano_id":       plano_id,
            "plano_nome":     r["plano_nome"],
            "plano_valor":    float(r["plano_valor"] or 0),
            "data_expiracao": str(r["data_expiracao"]),
            "telefone":       r["telefone"] or "",
            "cidade_nome":    r["cidade_nome"] or "",
            "status_alteracao": hub.get("status_alteracao", "pendente"),
            "obs":            hub.get("obs", ""),
            "responsavel":    hub.get("responsavel", ""),
            "novo_plano":     novo_plano,
        }

        if cidade and cidade.lower() not in item["cidade_nome"].lower(): continue
        if status and item["status_alteracao"] != status: continue
        resultado.append(item)

    total = len(resultado)
    return {
        "mes": mes,
        "kpis": {
            "total":      total,
            "receita":    round(sum(i["plano_valor"] for i in resultado), 2),
            "pendentes":  sum(1 for i in resultado if i["status_alteracao"] == "pendente"),
            "em_contato": sum(1 for i in resultado if i["status_alteracao"] == "em_contato"),
            "alterados":  sum(1 for i in resultado if i["status_alteracao"] == "alterado"),
            "recusou":    sum(1 for i in resultado if i["status_alteracao"] == "recusou"),
        },
        "contratos": resultado,
    }


class StatusPayload(BaseModel):
    status_alteracao: str
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
    contrato_id: int,
    payload: StatusPayload,
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    responsavel = usuario.get("nome", "") or usuario.get("login", "")
    existe = db.execute("SELECT id FROM hc_alteracao_planos WHERE ixc_contrato_id=?", (contrato_id,)).fetchone()
    if existe:
        db.execute("UPDATE hc_alteracao_planos SET status_alteracao=?,obs=?,responsavel=?,atualizado_em=? WHERE ixc_contrato_id=?",
                   (payload.status_alteracao, payload.obs, responsavel, agora, contrato_id))
    else:
        db.execute("""INSERT INTO hc_alteracao_planos
            (ixc_contrato_id,ixc_cliente_id,cliente,plano_nome,plano_valor,
             data_expiracao,telefone,cidade_nome,status_alteracao,obs,responsavel,criado_em,atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (contrato_id, payload.ixc_cliente_id, payload.cliente, payload.plano_nome,
             payload.plano_valor, payload.data_expiracao, payload.telefone, payload.cidade_nome,
             payload.status_alteracao, payload.obs, responsavel, agora, agora))
    db.commit()
    _notif_status(contrato_id, payload, responsavel)
    return {"ok": True, "contrato_id": contrato_id, "status": payload.status_alteracao}


class AplicarPayload(BaseModel):
    ixc_contrato_id:   int
    cliente:           str
    cidade_nome:       str
    plano_atual_id:    int
    plano_atual_nome:  str
    plano_atual_valor: float
    plano_novo_id:     int
    plano_novo_nome:   str
    plano_novo_valor:  float
    obs:               Optional[str] = ""

@router.post("/aplicar")
def aplicar_alteracao(
    payload: AplicarPayload,
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    # Valida de-para
    esperado = DEPARA_PLANOS.get(payload.plano_atual_id)
    if esperado != payload.plano_novo_id:
        raise HTTPException(status_code=400, detail="Plano novo não corresponde ao de-para configurado.")

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    responsavel = usuario.get("nome", "") or usuario.get("login", "")
    diferenca = round(payload.plano_novo_valor - payload.plano_atual_valor, 2)

    # Aplica no IXC
    try:
        with ixc_conn() as ixc:
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
    except Exception as e:
        log.error(f"Erro ao aplicar alteracao #{payload.ixc_contrato_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar no IXC: {e}")

    # Atualiza status local
    existe = db.execute("SELECT id FROM hc_alteracao_planos WHERE ixc_contrato_id=?", (payload.ixc_contrato_id,)).fetchone()
    if existe:
        db.execute("UPDATE hc_alteracao_planos SET status_alteracao='alterado',obs=?,responsavel=?,atualizado_em=? WHERE ixc_contrato_id=?",
                   (payload.obs, responsavel, agora, payload.ixc_contrato_id))
    else:
        db.execute("""INSERT INTO hc_alteracao_planos
            (ixc_contrato_id,ixc_cliente_id,cliente,plano_nome,plano_valor,
             cidade_nome,status_alteracao,obs,responsavel,criado_em,atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (payload.ixc_contrato_id, None, payload.cliente, payload.plano_atual_nome,
             payload.plano_atual_valor, payload.cidade_nome,
             "alterado", payload.obs, responsavel, agora, agora))
    db.commit()

    _notif_aplicado(payload, diferenca, responsavel)

    return {"ok": True, "msg": f"Plano alterado para {payload.plano_novo_nome} no IXC!",
            "diferenca": diferenca}


def _notif_status(contrato_id, payload, por):
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id: return
    icons  = {"pendente":"📋","em_contato":"📞","negociando":"🤝","confirmado":"✅","recusou":"❌"}
    labels = {"pendente":"PENDENTE","em_contato":"EM CONTATO","negociando":"NEGOCIANDO",
              "confirmado":"CONFIRMADO","recusou":"RECUSOU"}
    icon  = icons.get(payload.status_alteracao, "📋")
    label = labels.get(payload.status_alteracao, payload.status_alteracao.upper())
    msg = (f"{icon} *ALTERAÇÃO DE PLANO — {label}*\n\n"
           f"👤 *Cliente:* {payload.cliente}\n"
           f"📍 *Cidade:* {payload.cidade_nome}\n"
           f"📋 *Contrato:* #{contrato_id}\n"
           f"📦 *Plano atual:* {payload.plano_nome}\n")
    if payload.obs: msg += f"💬 *Obs:* {payload.obs}\n"
    msg += f"\n👨‍💼 *Por:* {por}"
    try:
        _req.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e:
        log.warning(f"Telegram status: {e}")


def _notif_aplicado(payload, diferenca, por):
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id: return
    sinal = "+" if diferenca >= 0 else ""
    msg = (f"⬆️ *PLANO ALTERADO — INTEGRADO IXC* ✅\n\n"
           f"👤 *Cliente:* {payload.cliente}\n"
           f"📍 *Cidade:* {payload.cidade_nome}\n"
           f"📋 *Contrato:* #{payload.ixc_contrato_id}\n\n"
           f"📦 *De:* {payload.plano_atual_nome}\n"
           f"🚀 *Para:* {payload.plano_novo_nome}\n"
           f"💰 *Diferença:* {sinal}R$ {diferenca:.2f}/mês\n\n"
           f"👨‍💼 *Por:* {por}")
    try:
        _req.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e:
        log.warning(f"Telegram aplicado: {e}")


@router.get("/buscar")
def buscar_global(
    q: str = Query(..., min_length=2),
    usuario=Depends(requer_backoffice()),
    db: sqlite3.Connection = Depends(get_db),
):
    termo = f"%{q}%"
    sql = """
        SELECT cc.id AS contrato_id, c.id AS cliente_id, c.razao AS cliente,
               cc.id_vd_contrato AS plano_id,
               cc.contrato AS plano_nome, vd.valor_contrato AS plano_valor,
               cc.data_expiracao, c.telefone_celular AS telefone,
               ci.nome AS cidade_nome
        FROM cliente_contrato cc
        INNER JOIN cliente c          ON c.id  = cc.id_cliente
        INNER JOIN vd_contratos vd    ON vd.id = cc.id_vd_contrato
        LEFT  JOIN cidade ci          ON ci.id = c.cidade
        WHERE cc.status = 'A'
          AND cc.data_expiracao >= CURDATE()
          AND (c.razao LIKE %s OR c.telefone_celular LIKE %s OR c.cnpj_cpf LIKE %s)
        ORDER BY cc.data_expiracao ASC
        LIMIT 50
    """
    with ixc_conn() as ixc:
        with ixc.cursor() as cur:
            cur.execute(sql, (termo, termo, termo))
            rows = cur.fetchall()

    ids = [r["contrato_id"] for r in rows]
    status_hub = {}
    if ids:
        ph = ",".join("?" * len(ids))
        rows_hub = db.execute(
            f"SELECT ixc_contrato_id, status_alteracao, obs, responsavel FROM hc_alteracao_planos WHERE ixc_contrato_id IN ({ph})", ids
        ).fetchall()
        for r in rows_hub:
            status_hub[r["ixc_contrato_id"]] = dict(r)

    resultado = []
    for r in rows:
        cid    = r["contrato_id"]
        hub    = status_hub.get(cid, {})
        pid    = r["plano_id"]
        novo_plano = None
        if pid in DEPARA_PLANOS:
            nid = DEPARA_PLANOS[pid]
            with ixc_conn() as ixc:
                with ixc.cursor() as cur:
                    cur.execute("SELECT id, nome, valor_contrato FROM vd_contratos WHERE id = %s", (nid,))
                    np = cur.fetchone()
                    if np:
                        novo_plano = {"id": np["id"], "nome": np["nome"], "valor": float(np["valor_contrato"] or 0)}
        resultado.append({
            "contrato_id":      cid,
            "cliente_id":       r["cliente_id"],
            "cliente":          r["cliente"],
            "plano_id":         pid,
            "plano_nome":       r["plano_nome"],
            "plano_valor":      float(r["plano_valor"] or 0),
            "data_expiracao":   str(r["data_expiracao"]),
            "telefone":         r["telefone"] or "",
            "cidade_nome":      r["cidade_nome"] or "",
            "status_alteracao": hub.get("status_alteracao", "pendente"),
            "obs":              hub.get("obs", ""),
            "responsavel":      hub.get("responsavel", ""),
            "novo_plano":       novo_plano,
        })

    return {"total": len(resultado), "contratos": resultado}
