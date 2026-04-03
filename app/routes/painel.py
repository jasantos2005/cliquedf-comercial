"""
Hub Comercial — app/routes/painel.py
Endpoints do painel interno web.
"""
import sqlite3, logging
from datetime import date, timedelta
from pathlib import Path
from fastapi import APIRouter, Depends, Query
from app.services.auth import requer_backoffice, requer_supervisor
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

def _mes(): return date.today().replace(day=1).strftime("%Y-%m-%d")
def _hoje(): return date.today().strftime("%Y-%m-%d")
def _data_6m(): return (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")


# ── DASHBOARD ────────────────────────────────────────────────
@router.get("/resumo")
async def resumo(db=Depends(get_db), user=Depends(requer_backoffice())):
    # Totais gerais
    totais = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(status='enviado' OR status='em_auditoria') AS em_andamento,
            SUM(status='pendente') AS pendentes,
            SUM(status='reprovado') AS reprovados,
            SUM(status='aprovado' OR status='assinatura_pendente') AS aguard_assinatura,
            SUM(status='assinado') AS assinados,
            SUM(status='ativado') AS ativados,
            SUM(status='erro_ativacao') AS erros,
            SUM(date(criado_em) = ?) AS hoje,
            SUM(date(criado_em) >= ?) AS mes
        FROM hc_precadastros
    """, (_hoje(), _mes())).fetchone()

    # Alertas
    alertas = db.execute("""
        SELECT
            SUM(status='pendente' AND atualizado_em <= datetime('now','-3 hours','-24 hours')) AS pendentes_urgentes,
            SUM(status='assinatura_pendente' AND atualizado_em <= datetime('now','-3 hours','-48 hours')) AS assinatura_atrasada,
            SUM(status='erro_ativacao') AS erros_ativacao
        FROM hc_precadastros
    """).fetchone()

    # Últimas atividades
    atividades = db.execute("""
        SELECT p.id, p.razao, p.status, p.plano_nome,
               u.nome AS vendedor, p.atualizado_em
        FROM hc_precadastros p
        LEFT JOIN hc_usuarios u ON u.id = p.id_vendedor_hub
        ORDER BY p.atualizado_em DESC LIMIT 10
    """).fetchall()

    # Funil do mês
    funil = db.execute("""
        SELECT
            COUNT(*) AS leads,
            SUM(status NOT IN ('reprovado')) AS passou_auditoria,
            SUM(status IN ('assinado','ativado')) AS assinou,
            SUM(status='ativado') AS ativado
        FROM hc_precadastros
        WHERE date(criado_em) >= ?
    """, (_mes(),)).fetchone()

    return {
        "totais":     dict(totais),
        "alertas":    dict(alertas),
        "atividades": [dict(a) for a in atividades],
        "funil":      dict(funil),
    }


# ── RANKING VENDEDORES ────────────────────────────────────────
@router.get("/ranking")
async def ranking(
    periodo: str = Query("mes", enum=["hoje","mes","trimestre","semestre"]),
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    datas = {
        "hoje":      _hoje(),
        "mes":       _mes(),
        "trimestre": (date.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
        "semestre":  _data_6m(),
    }
    inicio = datas[periodo]

    rows = db.execute("""
        SELECT
            u.id, u.nome,
            COUNT(p.id) AS leads,
            SUM(p.status NOT IN ('reprovado')) AS validos,
            SUM(p.status IN ('assinado','ativado')) AS convertidos,
            SUM(p.status='ativado') AS ativados,
            SUM(p.status='reprovado') AS reprovados,
            SUM(p.viabilidade_status='alerta') AS sem_viab
        FROM hc_usuarios u
        LEFT JOIN hc_precadastros p
            ON p.id_vendedor_hub = u.id
            AND date(p.criado_em) >= ?
        JOIN hc_grupos g ON g.id = u.id_grupo
        WHERE u.ativo = 1
        GROUP BY u.id
        ORDER BY ativados DESC, convertidos DESC
    """, (inicio,)).fetchall()

    resultado = []
    for i, r in enumerate(rows):
        d = dict(r)
        leads = d["leads"] or 0
        ativ  = d["ativados"] or 0
        conv  = d["convertidos"] or 0
        d["posicao"]        = i + 1
        d["taxa_conversao"] = round(conv / max(leads, 1) * 100, 1)
        d["taxa_ativacao"]  = round(ativ / max(leads, 1) * 100, 1)
        # Score qualidade simplificado para o ranking
        d["score"] = min(100, round(
            (ativ * 40 / max(leads, 1)) +
            (conv * 30 / max(leads, 1)) +
            (max(0, leads - d["reprovados"]) * 30 / max(leads, 1))
        ))
        resultado.append(d)

    return {"periodo": periodo, "vendedores": resultado}


# ── RESUMO FINANCEIRO ─────────────────────────────────────────
@router.get("/financeiro")
async def financeiro_resumo(db=Depends(get_db), user=Depends(requer_supervisor())):
    clientes = db.execute("""
        SELECT ixc_cliente_id, ixc_contrato_id, razao, plano_nome,
               id_vendedor_hub, atualizado_em
        FROM hc_precadastros
        WHERE status='ativado' AND ixc_cliente_id IS NOT NULL
          AND date(atualizado_em) >= ?
        ORDER BY atualizado_em DESC
    """, (_data_6m(),)).fetchall()

    em_dia = atraso = inadimplente = sem_dados = 0
    valor_atraso = 0.0
    lista = []

    for c in clientes:
        try:
            fat = ixc_select("""
                SELECT status, valor_aberto,
                       DATEDIFF(NOW(), data_vencimento) AS dias
                FROM ixcprovedor.fn_areceber
                WHERE id_cliente=%s AND status='A'
                ORDER BY data_vencimento ASC LIMIT 1
            """, (c["ixc_cliente_id"],))

            if not fat:
                em_dia += 1
                sit = "em_dia"; dias = 0; vab = 0
            else:
                dias = int(fat[0]["dias"] or 0)
                vab  = float(fat[0]["valor_aberto"] or 0)
                valor_atraso += vab
                if dias <= 0:    em_dia += 1;      sit = "em_dia"
                elif dias <= 15: atraso += 1;      sit = "atraso_leve"
                elif dias <= 30: atraso += 1;      sit = "atraso_medio"
                else:            inadimplente += 1; sit = "inadimplente"

            lista.append({**dict(c), "situacao": sit,
                          "dias_atraso": dias, "valor_aberto": round(vab,2)})
        except:
            sem_dados += 1

    return {
        "resumo": {"em_dia": em_dia, "atraso": atraso,
                   "inadimplente": inadimplente, "valor_total_atraso": round(valor_atraso,2)},
        "clientes": lista,
    }


# ── CANCELAMENTOS ─────────────────────────────────────────────
@router.get("/cancelamentos")
async def cancelamentos(db=Depends(get_db), user=Depends(requer_supervisor())):
    clientes = db.execute("""
        SELECT p.id, p.razao, p.plano_nome, p.plano_valor,
               p.ixc_contrato_id, p.atualizado_em AS data_ativacao,
               u.nome AS vendedor
        FROM hc_precadastros p
        LEFT JOIN hc_usuarios u ON u.id = p.id_vendedor_hub
        WHERE p.status='ativado' AND p.ixc_contrato_id IS NOT NULL
          AND date(p.atualizado_em) >= ?
    """, (_data_6m(),)).fetchall()

    cancelados = []
    for c in clientes:
        try:
            ct = ixc_select_one("""
                SELECT status, data_cancelamento, motivo_cancelamento, data
                FROM ixcprovedor.cliente_contrato
                WHERE id=%s AND status='C'
            """, (c["ixc_contrato_id"],))
            if not ct: continue
            from datetime import datetime
            da = datetime.strptime(c["data_ativacao"][:10], "%Y-%m-%d") if c["data_ativacao"] else None
            dc = ct["data_cancelamento"]
            perm = (dc - da.date()).days if da and dc and hasattr(dc,'day') else None
            cancelados.append({
                **dict(c),
                "data_cancelamento":   str(ct["data_cancelamento"]) if ct["data_cancelamento"] else None,
                "motivo_cancelamento": ct["motivo_cancelamento"],
                "permanencia_dias":    perm,
                "venda_ruim":          perm is not None and perm < 90,
            })
        except: pass

    return {"cancelados": cancelados, "total": len(cancelados)}
