"""
Hub Comercial — app/routes/vendedor_acompanhamento.py
Endpoints para os menus de acompanhamento do app do vendedor.

GET /api/vendedor/dashboard      → KPIs do vendedor
GET /api/vendedor/leads          → pré-cadastros do vendedor
GET /api/vendedor/clientes       → clientes ativados no IXC
GET /api/vendedor/financeiro     → situação financeira dos clientes
GET /api/vendedor/cancelamentos  → clientes cancelados
GET /api/vendedor/eficiencia     → performance mensal
POST /api/vendedor/reenviar-link/{id} → reenvia link de assinatura
"""
import sqlite3, logging, os
from datetime import datetime, date, timedelta
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from app.services.auth import requer_vendedor
from app.services.ixc_db import ixc_select, ixc_select_one

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "hub_comercial.db"
log      = logging.getLogger(__name__)
router   = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    try: yield c
    finally: c.close()

def _hoje(): return date.today().strftime("%Y-%m-%d")
def _mes_inicio(): return date.today().replace(day=1).strftime("%Y-%m-%d")
def _data_6m(): return (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")


# ── DASHBOARD ────────────────────────────────────────────────
@router.get("/dashboard")
async def dashboard(db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])

    leads_mes = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(status NOT IN ('reprovado')) AS ativos,
            SUM(status='enviado' OR status='em_auditoria') AS em_andamento,
            SUM(status='aprovado' OR status='assinatura_pendente') AS aguard_assinatura,
            SUM(status='assinado' OR status='ativado') AS convertidos,
            SUM(status='ativado') AS ativados,
            SUM(status='reprovado') AS reprovados,
            SUM(status='pendente') AS pendentes
        FROM hc_precadastros
        WHERE id_vendedor_hub=?
          AND date(criado_em) >= ?
    """, (vid, _mes_inicio())).fetchone()

    # Alertas: assinatura pendente há mais de 48h
    alertas = db.execute("""
        SELECT COUNT(*) AS qtd FROM hc_precadastros
        WHERE id_vendedor_hub=?
          AND status='assinatura_pendente'
          AND atualizado_em <= datetime('now','-3 hours','-48 hours')
    """, (vid,)).fetchone()

    # OS não instaladas há mais de 7 dias
    os_atrasadas = db.execute("""
        SELECT COUNT(*) AS qtd FROM hc_precadastros
        WHERE id_vendedor_hub=?
          AND status='ativado'
          AND ixc_os_id IS NOT NULL
          AND date(atualizado_em) <= date(datetime('now','-3 hours'),'-7 days')
    """, (vid,)).fetchone()

    return {
        "mes": {
            "total":           leads_mes["total"] or 0,
            "em_andamento":    leads_mes["em_andamento"] or 0,
            "aguard_assinatura": leads_mes["aguard_assinatura"] or 0,
            "convertidos":     leads_mes["convertidos"] or 0,
            "ativados":        leads_mes["ativados"] or 0,
            "reprovados":      leads_mes["reprovados"] or 0,
            "pendentes":       leads_mes["pendentes"] or 0,
        },
        "taxa_conversao": round(
            (leads_mes["convertidos"] or 0) / max(leads_mes["total"] or 1, 1) * 100, 1
        ),
        "alertas": {
            "assinatura_atrasada": alertas["qtd"] or 0,
            "os_nao_instalada":    os_atrasadas["qtd"] or 0,
        }
    }


# ── LEADS ─────────────────────────────────────────────────────
@router.get("/leads")
async def leads(
    status: str = "todos",
    db=Depends(get_db),
    user=Depends(requer_vendedor())
):
    vid = int(user["sub"])
    where = "id_vendedor_hub=?"
    params = [vid]
    if status != "todos":
        where += " AND status=?"
        params.append(status)

    rows = db.execute(f"""
        SELECT id, protocolo, status, razao, cnpj_cpf,
               plano_nome, cidade_nome, uf_sigla,
               viabilidade_status,
               token_assinatura, token_expira_em,
               ixc_cliente_id, ixc_contrato_id, ixc_os_id,
               criado_em, atualizado_em,
               (SELECT GROUP_CONCAT(a.legenda,' | ')
                FROM hc_auditoria_log a
                WHERE a.precadastro_id=hc_precadastros.id
                  AND a.resultado IN ('reprovado','pendente')
                ORDER BY a.id DESC LIMIT 2) AS motivo
        FROM hc_precadastros
        WHERE {where}
        ORDER BY criado_em DESC
        LIMIT 100
    """, params).fetchall()

    base_url = os.getenv("BASE_URL","https://comercial.iatechhub.cloud")
    result = []
    for r in rows:
        d = dict(r)
        if d.get("token_assinatura"):
            d["link_assinatura"] = f"{base_url}/assinar/{d['token_assinatura']}"
        result.append(d)

    return {"leads": result, "total": len(result)}


# ── CLIENTES ATIVADOS ─────────────────────────────────────────
@router.get("/clientes")
async def clientes_ativados(db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])

    rows = db.execute("""
        SELECT id, razao, cnpj_cpf, plano_nome, plano_valor,
               ixc_cliente_id, ixc_contrato_id, ixc_os_id,
               atualizado_em AS data_ativacao
        FROM hc_precadastros
        WHERE id_vendedor_hub=?
          AND status='ativado'
          AND date(atualizado_em) >= ?
        ORDER BY atualizado_em DESC
    """, (vid, _data_6m())).fetchall()

    clientes = []
    for r in rows:
        d = dict(r)

        # Buscar status da OS no IXC
        if d.get("ixc_os_id"):
            try:
                os_data = ixc_select_one("""
                    SELECT o.status, o.data_abertura, o.data_fechamento,
                           f.funcionario AS tecnico_nome,
                           DATEDIFF(NOW(), o.data_abertura) AS dias_aberta
                    FROM ixcprovedor.su_oss_chamado o
                    LEFT JOIN ixcprovedor.funcionarios f ON f.id=o.id_tecnico
                    WHERE o.id=%s
                """, (d["ixc_os_id"],))
                if os_data:
                    d["os_status"]       = os_data["status"]
                    d["os_tecnico"]      = os_data["tecnico_nome"]
                    d["os_data_fechamento"] = str(os_data["data_fechamento"]) if os_data["data_fechamento"] else None
                    d["os_dias_aberta"]  = os_data["dias_aberta"]
                    d["os_alerta"]       = (os_data["status"] == "A" and
                                            (os_data["dias_aberta"] or 0) > 7)
            except Exception as e:
                log.warning(f"OS {d['ixc_os_id']}: {e}")

        clientes.append(d)

    return {"clientes": clientes, "total": len(clientes)}


# ── FINANCEIRO ────────────────────────────────────────────────
@router.get("/financeiro")
async def financeiro(db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])

    rows = db.execute("""
        SELECT id, razao, ixc_cliente_id, ixc_contrato_id,
               plano_nome, plano_valor, atualizado_em AS data_ativacao
        FROM hc_precadastros
        WHERE id_vendedor_hub=?
          AND status='ativado'
          AND date(atualizado_em) >= ?
        ORDER BY atualizado_em DESC
    """, (vid, _data_6m())).fetchall()

    resultado = []
    for r in rows:
        d = dict(r)
        if not d.get("ixc_cliente_id"):
            continue
        try:
            faturas = ixc_select("""
                SELECT id, valor, data_vencimento, status,
                       valor_aberto,
                       DATEDIFF(NOW(), data_vencimento) AS dias_atraso
                FROM ixcprovedor.fn_areceber
                WHERE id_cliente=%s
                  AND data_vencimento >= %s
                ORDER BY data_vencimento DESC
                LIMIT 6
            """, (d["ixc_cliente_id"], _data_6m()))

            total_aberto  = sum(float(f["valor_aberto"] or 0) for f in faturas)
            dias_atraso   = max((int(f["dias_atraso"] or 0) for f in faturas if f["status"]=="A"), default=0)
            pagas         = sum(1 for f in faturas if f["status"]=="B")
            total_faturas = len(faturas)

            if dias_atraso == 0:
                situacao = "em_dia"
            elif dias_atraso <= 15:
                situacao = "atraso_leve"
            elif dias_atraso <= 30:
                situacao = "atraso_medio"
            else:
                situacao = "inadimplente"

            # Score da venda
            score = 0
            if d.get("ixc_os_id"): score += 30
            if pagas >= 3: score += 30
            if situacao == "em_dia": score += 20
            if dias_atraso > 90: score -= 20

            d["faturas"]       = [dict(f) for f in faturas]
            d["total_aberto"]  = round(total_aberto, 2)
            d["dias_atraso"]   = dias_atraso
            d["situacao"]      = situacao
            d["score_venda"]   = max(0, min(100, score))
            d["pagas"]         = pagas
            d["total_faturas"] = total_faturas
        except Exception as e:
            log.warning(f"Financeiro cliente {d['ixc_cliente_id']}: {e}")
            d["situacao"] = "sem_dados"
            d["score_venda"] = 0

        resultado.append(d)

    return {"clientes": resultado, "total": len(resultado)}


# ── CANCELAMENTOS ─────────────────────────────────────────────
@router.get("/cancelamentos")
async def cancelamentos(db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])

    rows = db.execute("""
        SELECT id, razao, plano_nome, plano_valor,
               ixc_cliente_id, ixc_contrato_id,
               atualizado_em AS data_ativacao
        FROM hc_precadastros
        WHERE id_vendedor_hub=? AND status='ativado'
          AND date(atualizado_em) >= ?
    """, (vid, _data_6m())).fetchall()

    cancelados = []
    for r in rows:
        d = dict(r)
        if not d.get("ixc_contrato_id"):
            continue
        try:
            ct = ixc_select_one("""
                SELECT status, data_cancelamento, motivo_cancelamento, data
                FROM ixcprovedor.cliente_contrato
                WHERE id=%s AND status='C'
            """, (d["ixc_contrato_id"],))
            if not ct:
                continue

            data_ativ   = datetime.strptime(d["data_ativacao"][:10], "%Y-%m-%d") if d["data_ativacao"] else None
            data_cancel = ct["data_cancelamento"]
            permanencia = None
            if data_ativ and data_cancel:
                permanencia = (data_cancel - data_ativ.date()).days if hasattr(data_cancel, 'day') else None

            d["data_cancelamento"]   = str(ct["data_cancelamento"]) if ct["data_cancelamento"] else None
            d["motivo_cancelamento"] = ct["motivo_cancelamento"]
            d["permanencia_dias"]    = permanencia
            d["venda_ruim"]          = permanencia is not None and permanencia < 90
            cancelados.append(d)
        except Exception as e:
            log.warning(f"Cancel {d['ixc_contrato_id']}: {e}")

    return {"cancelados": cancelados, "total": len(cancelados)}


# ── EFICIÊNCIA ────────────────────────────────────────────────
@router.get("/eficiencia")
async def eficiencia(db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])

    # Últimos 6 meses por mês
    meses = []
    hoje = date.today()
    for i in range(5, -1, -1):
        d = date(hoje.year, hoje.month, 1)
        mes = (d.month - i - 1) % 12 + 1
        ano = d.year - ((i + 1 - d.month) // 12 + (1 if (i + 1 - d.month) % 12 > 0 else 0))
        inicio = f"{ano}-{mes:02d}-01"
        if mes == 12:
            fim = f"{ano+1}-01-01"
        else:
            fim = f"{ano}-{mes+1:02d}-01"

        row = db.execute("""
            SELECT
                COUNT(*) AS leads,
                SUM(status IN ('assinado','ativado')) AS convertidos,
                SUM(status='ativado') AS ativados,
                SUM(status='reprovado') AS reprovados
            FROM hc_precadastros
            WHERE id_vendedor_hub=?
              AND date(criado_em) >= ? AND date(criado_em) < ?
        """, (vid, inicio, fim)).fetchone()

        meses.append({
            "mes":        f"{mes:02d}/{ano}",
            "leads":      row["leads"] or 0,
            "convertidos":row["convertidos"] or 0,
            "ativados":   row["ativados"] or 0,
            "reprovados": row["reprovados"] or 0,
            "taxa":       round((row["convertidos"] or 0) / max(row["leads"] or 1,1) * 100, 1),
        })

    # Score geral
    total_ativ = sum(m["ativados"] for m in meses)
    total_leads = sum(m["leads"] for m in meses)
    score_geral = round(total_ativ / max(total_leads, 1) * 100, 1)

    return {
        "meses":       meses,
        "score_geral": score_geral,
        "total_leads": total_leads,
        "total_ativados": total_ativ,
    }


# ── REENVIAR LINK ASSINATURA ──────────────────────────────────
@router.post("/reenviar-link/{id}")
async def reenviar_link(id: int, db=Depends(get_db), user=Depends(requer_vendedor())):
    vid = int(user["sub"])
    p = db.execute("""
        SELECT id, status, razao, telefone_celular, token_assinatura, token_expira_em
        FROM hc_precadastros WHERE id=? AND id_vendedor_hub=?
    """, (id, vid)).fetchone()

    if not p: raise HTTPException(404, "Não encontrado.")
    if p["status"] not in ("assinatura_pendente", "aprovado"):
        raise HTTPException(400, "Cadastro não está aguardando assinatura.")

    # Regenerar token se expirado
    from app.engines.contrato_engine import gerar_token_assinatura
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not p["token_assinatura"] or (p["token_expira_em"] and agora > p["token_expira_em"]):
        tk = gerar_token_assinatura(id)
        db.execute("""
            UPDATE hc_precadastros
            SET token_assinatura=?, token_expira_em=?,
                status='assinatura_pendente',
                atualizado_em=datetime('now','-3 hours')
            WHERE id=?
        """, (tk["token"], tk["expira_em"], id))
        db.commit()
        token = tk["token"]
    else:
        token = p["token_assinatura"]

    base_url = os.getenv("BASE_URL","https://comercial.iatechhub.cloud")
    link = f"{base_url}/assinar/{token}"
    cel  = (p["telefone_celular"] or "").replace(" ","").replace("(","").replace(")","").replace("-","")
    wpp  = f"https://wa.me/55{cel}?text=Olá! Segue o link para assinatura do seu contrato Cliquedf: {link}"

    return {"link": link, "whatsapp": wpp, "cliente": p["razao"]}
