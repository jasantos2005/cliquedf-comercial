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
from app.services.auth import requer_backoffice, requer_supervisor, requer_vendedor
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
    operador: str = Query(""),
    pagina:   int = Query(1, ge=1),
    db=Depends(get_db),
    user=Depends(requer_vendedor())
):
    """
    Lista clientes da base de upgrade com filtros.
    Retorna paginado (50 por página).
    """
    pp = 50
    offset = (pagina - 1) * pp
    where, params = ["plano_anterior_nome IS NOT NULL"], []

    if cidade:
        where.append("cidade = ?"); params.append(cidade)
    if plano:
        where.append("plano_nome LIKE ?"); params.append(f"%{plano}%")
    if status != "todos":
        where.append("status_negociacao = ?"); params.append(status)
    else:
        where.append("status_negociacao != 'cliente_ciente'")
    if busca:
        where.append("(cliente LIKE ? OR telefone_celular LIKE ? OR ixc_contrato_id LIKE ?)")
        params += [f"%{busca}%", f"%{busca}%", f"%{busca}%"]
    if operador:
        where.append("operador_contato = ?"); params.append(operador)

    w = " AND ".join(where)
    total = db.execute(f"SELECT COUNT(*) FROM hc_upgrades_base WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"""
        SELECT * FROM hc_upgrades_base
        WHERE {w}
        ORDER BY CASE WHEN plano_anterior_nome IS NOT NULL THEN 0 ELSE 1 END, cidade, cliente
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
async def detalhe_base(id: int, db=Depends(get_db), user=Depends(requer_vendedor())):
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
    user=Depends(requer_vendedor())
):
    """Atualiza o status de negociação de um cliente."""
    status_validos = {"nao_contatado", "em_contato", "negociando", "confirmado", "cliente_ciente", "recusou"}
    if payload.status_negociacao not in status_validos:
        raise HTTPException(400, f"Status inválido. Use: {status_validos}")

    row = db.execute("SELECT * FROM hc_upgrades_base WHERE id=?", (id,)).fetchone()
    if not row: raise HTTPException(404, "Não encontrado.")

    db.execute("""
        UPDATE hc_upgrades_base
        SET status_negociacao=?, obs_negociacao=?,
            operador_contato=?, data_contato=datetime('now','-3 hours')
        WHERE id=?
    """, (payload.status_negociacao, payload.obs_negociacao, user["login"], id))
    db.commit()

    # Notificar Telegram para qualquer mudança de status
    _notif_telegram_negociacao(dict(row), payload.status_negociacao,
                                payload.obs_negociacao or "", user["login"])
    return {"ok": True}


# ── Planos disponíveis no IXC ─────────────────────────────────

@router.get("/planos")
async def listar_planos_ixc(user=Depends(requer_vendedor())):
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
    base_id:           int
    plano_novo_id:     int
    plano_novo_nome:   str
    plano_novo_valor:  float
    obs:               Optional[str] = ""
    apenas_registrar:  bool = False

@router.post("/realizar")
async def realizar_upgrade(
    payload: UpgradePayload,
    db=Depends(get_db),
    user=Depends(requer_vendedor())
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

    # Se apenas_registrar=True (vendedor) nao aplica no IXC
    if payload.apenas_registrar:
        db.execute(
            "UPDATE hc_upgrades_base SET status_negociacao='confirmado',"
            " ixc_plano_id=?, plano_nome=? WHERE id=?",
            (payload.plano_novo_id, payload.plano_novo_nome, payload.base_id)
        )
        db.commit()
        _notif_telegram(p, plano_ant_nome, payload, diferenca, tipo_mudanca, user["login"])
        return {"ok": True, "tipo": tipo_mudanca, "diferenca": diferenca,
                "msg": "Negociacao registrada. Backoffice aplicara no IXC."}

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

        _notif_telegram(p, plano_ant_nome, payload, diferenca, tipo_mudanca, user["login"])
        log.info(
            f"Upgrade #{p['ixc_contrato_id']} — {p['cliente']} — "
            f"{plano_ant_nome} — {payload.plano_novo_nome} — por {user['login']}"
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
    user=Depends(requer_vendedor())
):
    """Histórico paginado de todos os upgrades realizados."""
    pp = 50
    offset = (pagina - 1) * pp
    where, params = ["plano_anterior_nome IS NOT NULL"], []

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
async def resumo_upgrades(db=Depends(get_db), user=Depends(requer_vendedor())):
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
            SUM(status_negociacao='cliente_ciente') AS cliente_ciente,
            SUM(status_negociacao='recusou')       AS recusou
        FROM hc_upgrades_base
        WHERE plano_anterior_nome IS NOT NULL
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
            "cliente_ciente": safe(base["cliente_ciente"]),
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


# ── Boletos do cliente ────────────────────────────────────────

@router.get("/base/{id}/boletos")
async def boletos_cliente(id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    row = db.execute("SELECT ixc_contrato_id FROM hc_upgrades_base WHERE id=?", (id,)).fetchone()
    if not row: raise HTTPException(404, "Não encontrado.")
    try:
        rows = ixc_select("""
            SELECT fn.id, fn.data_vencimento, fn.valor, fn.status,
                   fn.pagamento_data, vd.nome as plano_nome, vd.valor_contrato
            FROM fn_areceber fn
            LEFT JOIN cliente_contrato cc ON cc.id = fn.id_contrato
            LEFT JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
            WHERE fn.id_contrato = %s
            AND fn.data_vencimento BETWEEN '2026-04-01' AND '2026-05-31'
            ORDER BY fn.data_vencimento
        """, (row["ixc_contrato_id"],))
        def sv(v):
            if v is None: return None
            if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
            if hasattr(v,'strftime'): return str(v)
            return v
        return {"boletos": [{k: sv(val) for k,val in dict(r).items()} for r in rows]}
    except Exception as e:
        log.error(f"boletos_cliente: {e}")
        return {"boletos": []}


# ── Ranking operadores ────────────────────────────────────────

@router.get("/ranking-operadores")
async def ranking_operadores(
    de:  str = Query(""),
    ate: str = Query(""),
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    where, params = ["operador_contato IS NOT NULL"], []
    if de:
        where.append("date(data_contato) >= ?"); params.append(de)
    if ate:
        where.append("date(data_contato) <= ?"); params.append(ate)
    w = " AND ".join(where)
    rows = db.execute(f"""
        SELECT operador_contato as operador,
               COUNT(*) as total_contatos,
               SUM(status_negociacao='cliente_ciente') as clientes_cientes,
               SUM(status_negociacao='recusou') as recusaram,
               SUM(status_negociacao='em_contato') as em_contato,
               SUM(status_negociacao='negociando') as negociando,
               MAX(data_contato) as ultimo_contato
        FROM hc_upgrades_base
        WHERE {w}
        GROUP BY operador_contato
        ORDER BY clientes_cientes DESC, total_contatos DESC
    """, params).fetchall()
    return {"ranking": [dict(r) for r in rows]}


# ── Resumo por plano anterior ─────────────────────────────────

@router.get("/resumo-planos")
async def resumo_planos(db=Depends(get_db), user=Depends(requer_backoffice())):
    rows = db.execute("""
        SELECT plano_anterior_nome as plano, COUNT(*) as total,
               SUM(status_negociacao='nao_contatado') as nao_contatado,
               SUM(status_negociacao='em_contato') as em_contato,
               SUM(status_negociacao='negociando') as negociando,
               SUM(status_negociacao='cliente_ciente') as cliente_ciente,
               SUM(status_negociacao='recusou') as recusou
        FROM hc_upgrades_base WHERE plano_anterior_nome IS NOT NULL
        GROUP BY plano_anterior_nome ORDER BY total DESC
    """).fetchall()
    return {"planos": [dict(r) for r in rows]}

def _notif_telegram(p, plano_ant, payload, diferenca, tipo, por):
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    icon = "⬆️" if tipo == "upgrade" else "⬇️" if tipo == "downgrade" else "↔️"
    pendente = " _(aguard. integração IXC)_" if payload.apenas_registrar else " ✅ _Integrado no IXC_"
    msg = (
        f"{icon} *UPGRADE NEGOCIADO*{pendente}\n\n"
        f"👤 *Cliente:* {p.get('cliente','—')}\n"
        f"📍 *Cidade:* {p.get('cidade','—')}\n"
        f"📋 *Contrato:* #{p.get('ixc_contrato_id','—')}\n\n"
        f"📦 *De:* {plano_ant}\n"
        f"🚀 *Para:* {payload.plano_novo_nome}\n"
        f"💰 *Diferença:* {'+' if diferenca>=0 else ''}R$ {diferenca:.2f}/mês\n\n"
        f"👨‍💼 *Por:* {por}"
    )
    try:
        _req.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Telegram upgrade: {e}")

def _notif_telegram_negociacao(cl, status, obs, por):
    """Notifica o grupo comercial sobre qualquer mudança de status de negociação."""
    import requests as _req
    from dotenv import load_dotenv
    from pathlib import Path as _Path
    load_dotenv(_Path(__file__).resolve().parent.parent.parent / ".env")
    token   = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return

    icons = {
        "em_contato":    "📞",
        "negociando":    "🤝",
        "confirmado":    "✅",
        "cliente_ciente": "🎯",
        "recusou":       "❌",
        "nao_contatado": "📋",
    }
    labels = {
        "em_contato":    "EM CONTATO",
        "negociando":    "NEGOCIANDO",
        "confirmado":    "CONFIRMADO",
        "cliente_ciente": "CLIENTE CIENTE",
        "recusou":       "RECUSOU",
        "nao_contatado": "NÃO CONTATADO",
    }
    icon  = icons.get(status, "📋")
    label = labels.get(status, status.upper())

    msg = (
        f"{icon} *UPGRADE — {label}*\n\n"
        f"👤 *Cliente:* {cl.get('cliente','—')}\n"
        f"📍 *Cidade:* {cl.get('cidade','—')}\n"
        f"📦 *Plano atual:* {cl.get('plano_nome','—')}\n"
        f"📞 *Telefone:* {cl.get('telefone_celular') or cl.get('telefone_residencial','—')}\n"
    )
    if obs:
        msg += f"💬 *Obs:* {obs}\n"
    msg += f"\n👨‍💼 *Por:* {por}"

    try:
        _req.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Telegram negociacao: {e}")


# ── Buscar contrato no IXC por ID ─────────────────────────────

@router.get("/buscar-contrato/{contrato_id}")
async def buscar_contrato_ixc(contrato_id: int, db=Depends(get_db), user=Depends(requer_backoffice())):
    """Busca dados do contrato no IXC para adicionar na base de upgrade."""
    try:
        row = ixc_select_one("""
            SELECT
                cc.id AS ixc_contrato_id,
                cl.razao AS cliente,
                cl.cnpj_cpf,
                cl.telefone_celular,
                cl.whatsapp,
                ci.nome AS cidade,
                cc.id_vd_contrato AS ixc_plano_id,
                vd.nome AS plano_nome,
                vd.valor_contrato AS plano_valor
            FROM ixcprovedor.cliente_contrato cc
            JOIN ixcprovedor.cliente cl ON cl.id = cc.id_cliente
            LEFT JOIN ixcprovedor.cidade ci ON ci.id = cl.cidade
            LEFT JOIN ixcprovedor.vd_contratos vd ON vd.id = cc.id_vd_contrato
            WHERE cc.id = %s AND cc.status = 'A'
        """, (contrato_id,))

        if not row:
            raise HTTPException(404, "Contrato não encontrado ou inativo.")

        def sv(v):
            if v is None: return None
            if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal': return float(v)
            return v

        result = {k: sv(v) for k, v in dict(row).items()}

        # Verificar se já está na base ou no log
        existe_base = db.execute(
            'SELECT id FROM hc_upgrades_base WHERE ixc_contrato_id=?', (contrato_id,)
        ).fetchone()
        existe_log = db.execute(
            'SELECT plano_novo_nome, realizado_em FROM hc_upgrades_log WHERE ixc_contrato_id=? ORDER BY id DESC LIMIT 1',
            (contrato_id,)
        ).fetchone()

        if existe_base:
            result['aviso'] = 'ja_na_base'
            result['aviso_msg'] = 'Este contrato já está na base de upgrade (a realizar).'
        elif existe_log:
            result['aviso'] = 'ja_realizado'
            result['aviso_msg'] = f"Upgrade já realizado para {existe_log['plano_novo_nome']} em {existe_log['realizado_em'][:10]}."

        return result

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"buscar_contrato_ixc #{contrato_id}: {e}")
        raise HTTPException(500, str(e))


# ── Adicionar contrato manualmente na base de upgrade ─────────

class AdicionarBasePayload(BaseModel):
    ixc_contrato_id: int
    cliente:         str
    cidade:          str
    bairro:          Optional[str] = ""
    telefone_celular: Optional[str] = ""
    ixc_plano_id:    int
    plano_nome:      str
    dia_vencimento:  Optional[int] = 0

@router.post("/base/adicionar")
async def adicionar_base(
    payload: AdicionarBasePayload,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    """Adiciona manualmente um contrato na base de upgrade."""
    # Verificar se já existe na base ou no log
    existe_base = db.execute(
        "SELECT id, cliente FROM hc_upgrades_base WHERE ixc_contrato_id=?",
        (payload.ixc_contrato_id,)
    ).fetchone()
    if existe_base:
        raise HTTPException(400, f"Contrato #{payload.ixc_contrato_id} já está na base de upgrade.")

    existe_log = db.execute(
        "SELECT id, cliente, plano_novo_nome, realizado_em FROM hc_upgrades_log WHERE ixc_contrato_id=? ORDER BY id DESC LIMIT 1",
        (payload.ixc_contrato_id,)
    ).fetchone()
    if existe_log:
        raise HTTPException(400, f"Contrato #{payload.ixc_contrato_id} já possui upgrade realizado em {existe_log['realizado_em']} para {existe_log['plano_novo_nome']}.")

    db.execute("""
        INSERT INTO hc_upgrades_base (
            ixc_contrato_id, cliente, cidade, bairro,
            telefone_celular, ixc_plano_id, plano_nome,
            dia_vencimento, status_negociacao
        ) VALUES (?,?,?,?,?,?,?,?,'nao_contatado')
    """, (
        payload.ixc_contrato_id, payload.cliente, payload.cidade,
        payload.bairro or "", payload.telefone_celular or "",
        payload.ixc_plano_id, payload.plano_nome, payload.dia_vencimento or 0
    ))
    db.commit()
    log.info(f"Base upgrade: #{payload.ixc_contrato_id} {payload.cliente} adicionado por {user['login']}")
    return {"ok": True, "msg": f"Contrato #{payload.ixc_contrato_id} adicionado à base de upgrade."}
