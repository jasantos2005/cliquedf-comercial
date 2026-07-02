"""
Hub Comercial — app/routes/painel.py
Endpoints do painel interno web.
"""
import sqlite3, logging
from datetime import date, timedelta
from pathlib import Path
from fastapi import APIRouter, Depends, Query
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

def _mes(): return date.today().replace(day=1).strftime("%Y-%m-%d")
def _hoje(): return date.today().strftime("%Y-%m-%d")
def _data_6m(): return (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")


# ── DASHBOARD ────────────────────────────────────────────────
@router.get("/resumo")
async def resumo(
    de: str = Query(default=None), ate: str = Query(default=None),
    vendedor_id: str = Query(default=None), cidade: str = Query(default=None),
    bairro: str = Query(default=None),
    db=Depends(get_db), user=Depends(requer_backoffice())
):
    # Totais gerais
    # Monta filtros dinâmicos SQLite
    _w, _p = [], []
    _de  = de  or _mes()
    _ate = ate or _hoje()
    _w.append("date(criado_em) >= ?"); _p.append(_de)
    _w.append("date(criado_em) <= ?"); _p.append(_ate)
    if vendedor_id: _w.append("ixc_vendedor_id = ?"); _p.append(vendedor_id)
    if cidade:      _w.append("ixc_cidade_id = ?");    _p.append(cidade)
    if bairro:      _w.append("bairro = ?");           _p.append(bairro)
    _where = "WHERE " + " AND ".join(_w)
    totais = db.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(status='enviado' OR status='em_auditoria') AS em_andamento,
            SUM(status='pendente') AS pendentes,
            SUM(status='reprovado') AS reprovados,
            SUM(status='aprovado' OR status='assinatura_pendente') AS aguard_assinatura,
            SUM(status='assinado') AS assinados,
            SUM(status='ativado') AS ativados,
            SUM(status='erro_ativacao') AS erros,
            SUM(date(criado_em) = '{_hoje()}') AS hoje,
            SUM(date(criado_em) >= '{_mes()}') AS mes
        FROM hc_precadastros {_where}
    """, _p).fetchone()

    # Alertas
    alertas = db.execute(f"""
        SELECT
            SUM(status='pendente' AND atualizado_em <= datetime('now','-3 hours','-24 hours')) AS pendentes_urgentes,
            SUM(status='assinatura_pendente' AND atualizado_em <= datetime('now','-3 hours','-48 hours')) AS assinatura_atrasada,
            SUM(status='erro_ativacao') AS erros_ativacao
        FROM hc_precadastros {_where}
    """, _p).fetchone()

    # Totais IXC em tempo real
    with ixc_conn() as _c:
        with _c.cursor() as _cur:
            _ixc_sql = "SELECT SUM(cc.status_internet = 'A') AS ativados_ixc, SUM(cc.status_internet = 'AA') AS aguard_ass_ixc FROM cliente_contrato cc LEFT JOIN cliente c ON c.id = cc.id_cliente WHERE cc.data >= %s AND cc.data <= %s"
            _ixc_params = [_de, _ate]
            if vendedor_id: _ixc_sql += " AND cc.id_vendedor_ativ = %s"; _ixc_params.append(vendedor_id)
            if cidade:      _ixc_sql += " AND c.cidade = (SELECT id FROM cidade WHERE nome = %s LIMIT 1)"; _ixc_params.append(cidade)
            _cur.execute(_ixc_sql, _ixc_params)
            _ixc_totais = _cur.fetchone()
    ativados_ixc   = int(_ixc_totais['ativados_ixc']   or 0)
    aguard_ass_ixc = int(_ixc_totais['aguard_ass_ixc'] or 0)
    # Totais NV/TIT/RN separado
    with ixc_conn() as _c2:
        with _c2.cursor() as _cur2:
            _os_sql = ('SELECT SUM(o.id_assunto=227) AS nv,'
                       ' SUM(o.id_assunto=110) AS tit,'
                       ' SUM(o.id_assunto=75) AS rn'
                       ' FROM su_oss_chamado o'
                       ' JOIN cliente_contrato cc ON o.id_contrato_kit = cc.id'
                       ' JOIN cliente c ON c.id = cc.id_cliente'
                       ' WHERE o.status=%s AND cc.data >= %s AND cc.data <= %s')
            _os_p = ['F', _de, _ate]
            if vendedor_id: _os_sql += ' AND cc.id_vendedor_ativ = %s'; _os_p.append(vendedor_id)
            if cidade:      _os_sql += ' AND c.cidade = %s';            _os_p.append(cidade)
            _cur2.execute(_os_sql, _os_p)
            _os_r = _cur2.fetchone()
    total_nv  = int(_os_r['nv']  or 0)
    total_tit = int(_os_r['tit'] or 0)
    total_rn  = int(_os_r['rn']  or 0)
    aguard_ass_hub = int(dict(totais).get('aguard_assinatura') or 0)
    totais = dict(totais)
    totais['ativados']          = ativados_ixc
    totais['aguard_assinatura'] = aguard_ass_ixc
    # Últimas atividades — direto do IXC em tempo real
    _status_map = {
        'A': 'ativado', 'I': 'inativo', 'P': 'pre_contrato',
        'N': 'pendente', 'D': 'cancelado',
    }
    _status_internet_map = {
        'A': 'ativado', 'D': 'cancelado', 'CM': 'migrado',
        'CA': 'cancelado', 'CE': 'cancelado', 'FA': 'financeiro', 'AA': 'assinatura_pendente',
    }
    with ixc_conn() as _conn:
        with _conn.cursor() as _cur:
            _ativ_sql = """
                SELECT cc.id,
                       c.razao,
                       cc.status            AS status_contrato,
                       cc.status_internet   AS status_internet,
                       cc.contrato          AS plano_nome,
                       COALESCE(v.nome, '—') AS vendedor,
                       cc.data              AS data_cadastro,
                       os.data_fechamento   AS data_instalacao,
                       DATEDIFF(os.data_fechamento, cc.data) AS sla_dias,
                       os.id_assunto AS os_assunto
                FROM cliente_contrato cc
                INNER JOIN cliente c ON c.id = cc.id_cliente
                LEFT JOIN vendedor v ON v.id = cc.id_vendedor_ativ AND cc.id_vendedor_ativ > 0 AND cc.id_vendedor_ativ != 29
                LEFT JOIN su_oss_chamado os ON os.id_contrato_kit = cc.id
                    AND os.id_assunto IN (227, 110, 75) AND os.status = 'F' AND os.data_fechamento IS NOT NULL
                WHERE cc.data >= %s AND cc.data <= %s"""
            _ativ_params = [_de, _ate]
            if vendedor_id: _ativ_sql += " AND cc.id_vendedor_ativ = %s"; _ativ_params.append(vendedor_id)
            if cidade:      _ativ_sql += " AND c.cidade = %s";            _ativ_params.append(cidade)
            if bairro:      _ativ_sql += " AND cc.bairro = %s";           _ativ_params.append(bairro)
            _ativ_sql += " ORDER BY cc.id DESC LIMIT 20"
            _cur.execute(_ativ_sql, _ativ_params)
            _rows = _cur.fetchall()
    atividades = [
        {
            'id':            r['id'],
            'razao':         r['razao'],
            'status':        _status_internet_map.get(r['status_internet'] or '', None)
                             or _status_map.get(r['status_contrato'] or '', 'pendente'),
            'status_contrato':  r['status_contrato'] or '',
            'status_internet':  r['status_internet'] or '',
            'plano_nome':    r['plano_nome'],
            'vendedor':      r['vendedor'],
            'data_cadastro':  str(r['data_cadastro']) if r['data_cadastro'] else '',
            'data_instalacao': str(r['data_instalacao'])[:10] if r['data_instalacao'] else None,
            'sla_dias':      r['sla_dias'],
            'tipo_os':       'NV' if r.get('os_assunto') == 227 else ('TIT' if r.get('os_assunto') == 110 else ('RN' if r.get('os_assunto') == 75 else None)),
        }
        for r in _rows
    ]

    # Funil do mês
    funil = db.execute(f"""
        SELECT
            COUNT(*) AS leads,
            SUM(status NOT IN ('reprovado')) AS passou_auditoria,
            SUM(status IN ('assinado','ativado')) AS assinou,
            SUM(status='ativado') AS ativado
        FROM hc_precadastros {_where}
    """, _p).fetchone()
    funil = dict(funil)
    funil['ativado'] = ativados_ixc
    funil['nv']  = total_nv
    funil['tit'] = total_tit
    funil['rn']  = total_rn

    def _safe(d):
        return {k: (v if v is not None else 0) for k, v in dict(d).items()}

    return {
        "totais":     _safe(totais),
        "alertas":    _safe(alertas),
        "atividades": atividades,
        "funil":      _safe(funil),
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
            u.id, u.nome, u.ixc_funcionario_id,
            COUNT(p.id) AS leads,
            SUM(p.status NOT IN ('reprovado')) AS validos,
            SUM(p.status IN ('assinado','ativado')) AS convertidos,
            SUM(p.status='ativado') AS ativados_hub,
            SUM(p.status='reprovado') AS reprovados,
            SUM(p.viabilidade_status='alerta') AS sem_viab
        FROM hc_usuarios u
        LEFT JOIN hc_precadastros p
            ON p.id_vendedor_hub = u.id
            AND date(p.criado_em) >= ?
        JOIN hc_grupos g ON g.id = u.id_grupo
        WHERE u.ativo = 1
        GROUP BY u.id
    """, (inicio,)).fetchall()
    resultado = []
    for i, r in enumerate(rows):
        d = dict(r)
        ixc_id = d.get("ixc_funcionario_id")
        ativados_ixc = 0
        if ixc_id:
            row_ixc = db.execute(
                "SELECT COUNT(*) as t FROM hc_contratos_cache WHERE vendedor_id=? AND date(data_contrato)>=?",
                (ixc_id, inicio)
            ).fetchone()
            ativados_ixc = int(row_ixc["t"] or 0) if row_ixc else 0
        leads = int(d["leads"] or 0)
        ativ  = ativados_ixc if ativados_ixc > 0 else int(d["ativados_hub"] or 0)
        conv  = int(d["convertidos"] or 0)
        d["ativados"]       = ativ
        d["posicao"]        = i + 1
        d["taxa_conversao"] = round(conv / max(leads, 1) * 100, 1)
        d["taxa_ativacao"]  = round(ativ / max(leads, 1) * 100, 1)
        d["score"] = min(100, round(
            (ativ * 40 / max(leads, 1)) +
            (conv * 30 / max(leads, 1)) +
            (max(0, leads - (d["reprovados"] or 0)) * 30 / max(leads, 1))
        ))
        resultado.append(d)
    resultado.sort(key=lambda x: x["ativados"], reverse=True)
    for i, d in enumerate(resultado): d["posicao"] = i + 1
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

    # Buscar todos os financeiros de uma vez (evita N queries no MySQL)
    ids = [row["ixc_cliente_id"] for row in clientes if row["ixc_cliente_id"]]
    fat_map = {}
    if ids:
        try:
            placeholders = ",".join(["%s"] * len(ids))
            fats = ixc_select(f"""
                SELECT id_cliente, valor_aberto,
                       DATEDIFF(NOW(), data_vencimento) AS dias
                FROM ixcprovedor.fn_areceber
                WHERE id_cliente IN ({placeholders}) AND status='A'
                ORDER BY data_vencimento ASC
            """, tuple(ids))
            for fat in fats:
                cid = fat["id_cliente"]
                if cid not in fat_map:
                    fat_map[cid] = fat
        except:
            pass

    for c in clientes:
        try:
            fat = fat_map.get(c["ixc_cliente_id"])
            if not fat:
                em_dia += 1
                sit = "em_dia"; dias = 0; vab = 0
            else:
                dias = int(fat["dias"] or 0)
                vab  = float(fat["valor_aberto"] or 0)
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


# ── VENDAS IXC (desde 01/01/2026) ────────────────────────────
@router.get("/vendas-ixc")
async def vendas_ixc(
    periodo: str = Query("mes"),
    vendedor: str = Query(""),
    cidade: str = Query(""),
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    from datetime import date
    datas = {
        "mes":       date.today().replace(day=1).strftime("%Y-%m-%d"),
        "trimestre": (date.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
        "semestre":  (date.today() - timedelta(days=180)).strftime("%Y-%m-%d"),
        "2026":      "2026-01-01",
    }
    inicio = datas.get(periodo, "2026-01-01")

    filtro_vend = f"AND cc.id_vendedor_ativ = {int(vendedor)}" if vendedor else ""
    filtro_cid  = f"AND c.cidade = {int(cidade)}" if cidade and cidade.isdigit() else ""

    try:
        from app.services.ixc_db import ixc_conn
        with ixc_conn() as _conn:
            _cur = _conn.cursor()
            _sql = f"""
                SELECT cc.id, cc.data, cc.status, cc.status_internet,
                       c.razao, c.cnpj_cpf,
                       v.nome AS vendedor_nome,
                       ci.nome AS cidade_nome,
                       vc.nome AS plano_nome, vc.valor_contrato AS valor,
                       o.status AS os_status, f.funcionario AS tecnico_nome,
                       CASE WHEN o.data_fechamento IS NULL OR o.data_fechamento='0000-00-00 00:00:00' THEN 1 ELSE 0 END AS os_aberta
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id = cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.cidade ci ON ci.id = c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id = cc.id_vd_contrato
                LEFT JOIN ixcprovedor.su_oss_chamado o ON o.id_contrato_kit = cc.id AND o.id_assunto = 227
                LEFT JOIN ixcprovedor.funcionarios f ON f.id = o.id_tecnico
                WHERE cc.data_ativacao >= %s AND cc.status = 'A'
                  {filtro_vend} {filtro_cid}
                ORDER BY cc.id DESC LIMIT 500
            """
            _cur.execute(_sql, (inicio,))
            rows = _cur.fetchall()

        def _sv(v):
            if v is None: return None
            if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
            if hasattr(v,'isoformat'): return str(v)
            return v

        return {"vendas": [{k:_sv(val) for k,val in dict(r).items()} for r in rows]}
    except Exception as e:
        log.error(f"vendas-ixc erro: {e}")
        return {"vendas": []}


# ── FILTROS (vendedores e cidades do IXC) ────────────────────
@router.get("/filtros")
async def filtros(user=Depends(requer_backoffice())):
    vendedores = ixc_select("""
        SELECT DISTINCT v.id, v.nome
        FROM ixcprovedor.vendedor v
        JOIN ixcprovedor.cliente_contrato cc ON cc.id_vendedor_ativ = v.id
        WHERE cc.data >= '2026-01-01' AND v.nome IS NOT NULL
          AND v.id NOT IN (29) AND v.id > 0
        ORDER BY v.nome
    """)
    cidades = ixc_select("""
        SELECT DISTINCT ci.id, ci.nome
        FROM ixcprovedor.cidade ci
        JOIN ixcprovedor.cliente c ON c.cidade = ci.id
        JOIN ixcprovedor.cliente_contrato cc ON cc.id_cliente = c.id
        WHERE cc.data >= '2026-01-01' AND ci.nome IS NOT NULL
        ORDER BY ci.nome
    """)
    bairros = ixc_select("""
        SELECT DISTINCT c.bairro, ci.nome AS cidade
        FROM ixcprovedor.cliente c
        JOIN ixcprovedor.cliente_contrato cc ON cc.id_cliente = c.id
        LEFT JOIN ixcprovedor.cidade ci ON ci.id = c.cidade
        WHERE cc.data >= '2026-01-01'
          AND c.bairro IS NOT NULL AND c.bairro != ''
        ORDER BY c.bairro
    """)
    return {
        "vendedores": [dict(r) for r in vendedores],
        "cidades":    [dict(r) for r in cidades],
        "bairros":    [dict(r) for r in bairros],
    }


# ── VENDAS POR CIDADE (dashboard + página) ───────────────────
@router.get("/cidades")
async def cidades(
    periodo: str = Query("2026"),
    vendedor_id: str = Query(""),
    de: str = Query(""),
    ate: str = Query(""),
    user=Depends(requer_backoffice())
):
    datas = {
        "hoje":      _hoje(),
        "mes":       _mes(),
        "trimestre": (date.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
        "semestre":  _data_6m(),
        "2026":      "2026-01-01",
    }
    inicio = de if de else datas.get(periodo, "2026-01-01")
    fim    = ate if ate else _hoje()
    filtro_vend = f"AND c.id_vendedor = {int(vendedor_id)}" if vendedor_id else ""

    rows = ixc_select(f"""
        SELECT
            ci.id AS cidade_id,
            ci.nome AS cidade,
            COUNT(DISTINCT cc.id) AS total,
            SUM(CASE WHEN o.status='F' THEN 1 ELSE 0 END) AS instalados,
            SUM(CASE WHEN o.status='A' THEN 1 ELSE 0 END) AS pendentes,
            SUM(CASE WHEN cc.status='C' THEN 1 ELSE 0 END) AS cancelados,
            COUNT(DISTINCT cc.id_vendedor_ativ) AS qtd_vendedores
        FROM ixcprovedor.cliente_contrato cc
        JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
        LEFT JOIN ixcprovedor.cidade ci ON ci.id = c.cidade
        LEFT JOIN ixcprovedor.su_oss_chamado o
            ON o.id_contrato_kit = cc.id AND o.id_assunto = 227
        WHERE cc.data_ativacao >= %s AND cc.data_ativacao <= %s
          AND cc.status = 'A'
          {filtro_vend}
        GROUP BY ci.id, ci.nome
        ORDER BY total DESC
    """, (inicio, fim))

    return {"cidades": [dict(r) for r in rows], "periodo": periodo}


# ── DETALHE DE UMA CIDADE ─────────────────────────────────────
@router.get("/cidades/{cidade_id}")
async def detalhe_cidade(
    cidade_id: int,
    periodo: str = Query("2026"),
    user=Depends(requer_backoffice())
):
    datas = {
        "mes": _mes(), "trimestre": (date.today()-timedelta(days=90)).strftime("%Y-%m-%d"),
        "semestre": _data_6m(), "2026": "2026-01-01",
    }
    inicio = datas.get(periodo, "2026-01-01")

    # Info da cidade
    cidade = ixc_select_one("SELECT id, nome FROM ixcprovedor.cidade WHERE id=%s", (cidade_id,))

    # Contratos detalhados
    contratos = ixc_select("""
        SELECT cc.id, cc.data, cc.status,
               c.razao, c.cnpj_cpf, c.bairro,
               v.nome AS vendedor_nome,
               vc.nome AS plano_nome, vc.valor_contrato AS valor,
               o.status AS os_status,
               f.funcionario AS tecnico_nome,
               o.data_fechamento AS os_fechamento
        FROM ixcprovedor.cliente_contrato cc
        JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
        LEFT JOIN ixcprovedor.vendedor v ON v.id = c.id_vendedor
        LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id = cc.id_vd_contrato
        LEFT JOIN ixcprovedor.su_oss_chamado o
            ON o.id_cliente = c.id AND o.id_assunto = 227
        LEFT JOIN ixcprovedor.funcionarios f ON f.id = o.id_tecnico
        WHERE c.cidade = %s AND cc.data >= %s AND c.id_vendedor > 0
        ORDER BY cc.id DESC
    """, (cidade_id, inicio))

    # Vendedores nessa cidade
    vendedores = ixc_select("""
        SELECT v.nome, COUNT(DISTINCT cc.id) AS total,
               SUM(CASE WHEN o.status='F' THEN 1 ELSE 0 END) AS instalados
        FROM ixcprovedor.cliente_contrato cc
        JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
        JOIN ixcprovedor.vendedor v ON v.id = c.id_vendedor
        LEFT JOIN ixcprovedor.su_oss_chamado o
            ON o.id_cliente = c.id AND o.id_assunto = 227
        WHERE c.cidade = %s AND cc.data >= %s
        GROUP BY v.nome ORDER BY total DESC
    """, (cidade_id, inicio))

    # Bairros nessa cidade
    bairros = ixc_select("""
        SELECT c.bairro, COUNT(*) AS total,
               SUM(CASE WHEN o.status='F' THEN 1 ELSE 0 END) AS instalados
        FROM ixcprovedor.cliente_contrato cc
        JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
        LEFT JOIN ixcprovedor.su_oss_chamado o
            ON o.id_cliente = c.id AND o.id_assunto = 227
        WHERE c.cidade = %s AND cc.data >= %s
          AND c.bairro IS NOT NULL AND c.bairro != ''
        GROUP BY c.bairro ORDER BY total DESC LIMIT 15
    """, (cidade_id, inicio))

    # Planos nessa cidade
    planos = ixc_select("""
        SELECT vc.nome AS plano, COUNT(*) AS total
        FROM ixcprovedor.cliente_contrato cc
        JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
        LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id = cc.id_vd_contrato
        WHERE c.cidade = %s AND cc.data >= %s AND c.id_vendedor > 0
        GROUP BY vc.nome ORDER BY total DESC
    """, (cidade_id, inicio))

    def proc(rows):
        result = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if hasattr(v, '__class__') and v.__class__.__name__ in ('Decimal',):
                    d[k] = float(v)
                elif hasattr(v, 'isoformat'):
                    d[k] = str(v)
            result.append(d)
        return result

    return {
        "cidade":    dict(cidade) if cidade else {},
        "contratos": proc(contratos),
        "vendedores":proc(vendedores),
        "bairros":   proc(bairros),
        "planos":    proc(planos),
        "periodo":   periodo,
    }


# ── EVOLUÇÃO DOS CONTRATOS ────────────────────────────────────
@router.get("/evolucao")
async def evolucao(user=Depends(requer_backoffice())):
    from app.services.ixc_db import ixc_conn
    with ixc_conn() as conn:
        cur = conn.cursor()

        # Diário — últimos 30 dias
        cur.execute("""
            SELECT DATE(cc.data) as dia, COUNT(*) as ativados
            FROM ixcprovedor.cliente_contrato cc
            JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
            WHERE cc.data >= DATE_SUB(NOW(), INTERVAL 30 DAY)
              AND c.id_vendedor > 0
            GROUP BY DATE(cc.data) ORDER BY dia
        """)
        diario = [{"dia": str(r["dia"]), "ativados": r["ativados"]} for r in cur.fetchall()]

        # Mensal — 2025 e 2026
        cur.execute("""
            SELECT DATE_FORMAT(cc.data, "%Y-%m") as mes, COUNT(*) as ativados
            FROM ixcprovedor.cliente_contrato cc
            JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
            WHERE cc.data >= "2025-01-01" AND c.id_vendedor > 0
            GROUP BY mes ORDER BY mes
        """)
        mensal = [{"mes": r["mes"], "ativados": r["ativados"]} for r in cur.fetchall()]

        # Anual — histórico completo
        cur.execute("""
            SELECT YEAR(cc.data) as ano, COUNT(*) as ativados
            FROM ixcprovedor.cliente_contrato cc
            JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
            WHERE c.id_vendedor > 0 AND YEAR(cc.data) >= 2021
            GROUP BY ano ORDER BY ano
        """)
        anual = [{"ano": r["ano"], "ativados": r["ativados"]} for r in cur.fetchall()]

        # Média mensal por vendedor no ano atual
        cur.execute("""
            SELECT DATE_FORMAT(cc.data, "%Y-%m") as mes,
                   v.nome as vendedor, COUNT(*) as total
            FROM ixcprovedor.cliente_contrato cc
            JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
            JOIN ixcprovedor.vendedor v ON v.id=c.id_vendedor
            WHERE cc.data >= "2026-01-01" AND c.id_vendedor > 0
            GROUP BY mes, v.nome ORDER BY mes, total DESC
        """)
        por_vendedor = [{"mes": r["mes"], "vendedor": r["vendedor"], "total": r["total"]} for r in cur.fetchall()]

    return {
        "diario":       diario,
        "mensal":       mensal,
        "anual":        anual,
        "por_vendedor": por_vendedor,
    }


# ── AUTOMACOES LOG ────────────────────────────────────────────
from fastapi import Response as FastAPIResponse
import sqlite3 as _sqlite3

@router.get("/automacoes")
async def automacoes_lista(
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    # Ultimo log de cada motor
    ultimos = db.execute("""
        SELECT motor, status, resumo, duracao_s, linhas, criado_em,
               ROW_NUMBER() OVER(PARTITION BY motor ORDER BY id DESC) as rn
        FROM hc_automacoes_log
    """).fetchall()
    ultimos = [dict(r) for r in ultimos if r["rn"] == 1]

    # Historico geral (ultimas 50 execucoes)
    historico = db.execute("""
        SELECT id, motor, status, resumo, duracao_s, linhas, criado_em
        FROM hc_automacoes_log
        ORDER BY id DESC LIMIT 50
    """).fetchall()

    return {
        "motores": ultimos,
        "historico": [dict(r) for r in historico]
    }

@router.get("/automacoes/{log_id}/texto")
async def automacao_texto(
    log_id: int,
    db=Depends(get_db),
    user=Depends(requer_backoffice())
):
    r = db.execute("SELECT motor, log_texto, criado_em FROM hc_automacoes_log WHERE id=?", (log_id,)).fetchone()
    if not r: raise HTTPException(404, "Log não encontrado.")
    return {"motor": r["motor"], "log_texto": r["log_texto"] or "", "criado_em": r["criado_em"]}


# ── RANKING IXC (direto do IXC) ───────────────────────────────
@router.get("/ranking-ixc")
async def ranking_ixc(
    de: str = Query(""),
    ate: str = Query(""),
    cidade: str = Query(""),
    user=Depends(requer_backoffice())
):
    from datetime import date
    _de  = de  or date.today().replace(day=1).strftime("%Y-%m-%d")
    _ate = ate or date.today().strftime("%Y-%m-%d")
    try:
        from app.services.ixc_db import ixc_conn
        with ixc_conn() as conn:
            cur = conn.cursor()
            sql = """
                SELECT v.nome AS vendedor, COUNT(DISTINCT cc.id) AS total,
                       SUM(cc.status_internet='A') AS ativos,
                       SUM(o.status='F' AND o.id_assunto=227) AS nv,
                       SUM(o.status='F' AND o.id_assunto=110) AS tit,
                       SUM(o.status='F' AND o.id_assunto=75)  AS rn
                FROM cliente_contrato cc
                JOIN cliente c ON c.id = cc.id_cliente
                JOIN vendedor v ON v.id = cc.id_vendedor_ativ
                LEFT JOIN su_oss_chamado o ON o.id_contrato_kit = cc.id
                    AND o.id_assunto IN (227,110,75) AND o.status='F'
                WHERE cc.data >= %s AND cc.data <= %s
                  AND cc.id_vendedor_ativ > 0
                  AND cc.id_vendedor_ativ != 29
            """
            params = [_de, _ate]
            if cidade:
                sql += " AND c.cidade = %s"
                params.append(cidade)
            sql += " GROUP BY v.nome ORDER BY total DESC LIMIT 10"
            cur.execute(sql, params)
            rows = cur.fetchall()
        return {"vendedores": [
            {"nome": r["vendedor"], "total": r["total"],
             "ativos": int(r["ativos"] or 0),
             "nv": int(r["nv"] or 0),
             "tit": int(r["tit"] or 0),
             "rn": int(r["rn"] or 0)}
            for r in rows
        ]}
    except Exception as e:
        log.error(f"ranking-ixc: {e}")
        return {"vendedores": []}


@router.get("/auditoria-ixc")
async def auditoria_ixc(
    de: str = Query("2026-01-01"),
    ate: str = Query(""),
    vendedor_id: str = Query(""),
    cidade: str = Query(""),
    nivel: str = Query(""),
    regra: str = Query(""),
    user=Depends(requer_backoffice())
):
    from app.engines.auditoria_ixc_engine import auditar_contratos
    lista = auditar_contratos(de, ate or None, vendedor_id, cidade)
    if nivel:
        lista = [c for c in lista if c["nivel_max"] == nivel]
    if regra:
        lista = [c for c in lista if any(p["regra"] == regra for p in c["problemas"])]
    def sv(v):
        if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
        if hasattr(v,'isoformat'): return str(v)
        return v
    return {
        "contratos": [{k:sv(val) for k,val in c.items()} for c in lista],
        "total": len(lista),
        "por_nivel": {
            "critico": sum(1 for c in lista if c["nivel_max"]=="critico"),
            "grave":   sum(1 for c in lista if c["nivel_max"]=="grave"),
            "alerta":  sum(1 for c in lista if c["nivel_max"]=="alerta"),
        }
    }

@router.get("/auditoria-ixc/resumo")
async def auditoria_ixc_resumo(
    de: str = Query("2026-01-01"),
    user=Depends(requer_backoffice())
):
    from app.engines.auditoria_ixc_engine import resumo_auditoria
    return resumo_auditoria(de)


@router.get("/sem-instalacao")
async def sem_instalacao(
    de: str = Query("2026-01-01"),
    ate: str = Query(""),
    vendedor_id: str = Query(""),
    cidade: str = Query(""),
    user=Depends(requer_backoffice())
):
    from datetime import date
    _ate = ate or date.today().strftime("%Y-%m-%d")
    filtro_vend = f"AND cc.id_vendedor_ativ = {int(vendedor_id)}" if vendedor_id else ""
    filtro_cid  = f"AND c.cidade = {int(cidade)}" if cidade and str(cidade).isdigit() else ""
    try:
        from app.services.ixc_db import ixc_conn
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT cc.id AS contrato_id,
                       c.razao, c.cnpj_cpf, c.telefone_celular,
                       ci.nome AS cidade_nome, c.bairro,
                       v.nome AS vendedor_nome,
                       vc.nome AS plano_nome,
                       cc.data_ativacao,
                       DATEDIFF(NOW(), cc.data_ativacao) AS dias
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
                LEFT JOIN ixcprovedor.cidade ci ON ci.id = c.cidade
                LEFT JOIN ixcprovedor.vendedor v ON v.id = cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id = cc.id_vd_contrato
                LEFT JOIN ixcprovedor.su_oss_chamado o ON o.id_contrato_kit = cc.id
                    AND o.id_assunto IN (227,110,75,15) AND o.status = 'F'
                WHERE cc.data_ativacao >= %s AND cc.data_ativacao <= %s
                  AND cc.status = 'A' AND o.id IS NULL
                  {filtro_vend} {filtro_cid}
                ORDER BY dias DESC
            """, (de, _ate))
            rows = cur.fetchall()

        def sv(v):
            if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
            if hasattr(v,'isoformat'): return str(v)
            return v

        contratos = [{k:sv(val) for k,val in dict(r).items()} for r in rows]
        criticos = sum(1 for c in contratos if (c.get('dias') or 0) > 30)
        graves   = sum(1 for c in contratos if 7 < (c.get('dias') or 0) <= 30)
        alertas  = sum(1 for c in contratos if 1 <= (c.get('dias') or 0) <= 7)

        return {
            "contratos": contratos,
            "total": len(contratos),
            "criticos": criticos,
            "graves": graves,
            "alertas": alertas,
        }
    except Exception as e:
        log.error(f"sem-instalacao: {e}")
        return {"contratos":[],"total":0,"criticos":0,"graves":0,"alertas":0}


# ── VENDEDORES ────────────────────────────────────────────────
@router.get("/vendedores")
async def vendedores_lista(db=Depends(get_db), user=Depends(requer_backoffice())):
    # Buscar IDs autorizados da tabela SQLite
    ativos = db.execute("SELECT ixc_id FROM hc_vendedores_ativos WHERE ativo=1").fetchall()
    ids = [str(r["ixc_id"]) for r in ativos]
    if not ids:
        return {"vendedores": []}
    ids_str = ",".join(ids)
    from app.services.ixc_db import ixc_conn
    with ixc_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT v.id, v.nome, COUNT(DISTINCT cc.id) as total
            FROM ixcprovedor.vendedor v
            JOIN ixcprovedor.cliente_contrato cc ON cc.id_vendedor_ativ = v.id
            WHERE cc.data_ativacao >= '2026-01-01' AND cc.status = 'A'
              AND v.id IN ({ids_str})
            GROUP BY v.id, v.nome ORDER BY v.nome
        """)
        return {"vendedores": [dict(r) for r in cur.fetchall()]}


@router.get("/vendedores/{vid}/produtividade")
async def vendedor_produtividade(
    vid: int,
    de: str = Query("2026-01-01"),
    ate: str = Query(""),
    user=Depends(requer_backoffice())
):
    from datetime import date
    from app.services.ixc_db import ixc_conn
    _ate = ate or date.today().strftime("%Y-%m-%d")

    with ixc_conn() as conn:
        cur = conn.cursor()

        # Info do vendedor
        cur.execute("SELECT id, nome FROM ixcprovedor.vendedor WHERE id=%s", (vid,))
        vend = cur.fetchone()

        # KPIs gerais
        cur.execute("""
            SELECT COUNT(DISTINCT cc.id) as total,
                   SUM(cc.status_internet='A') as ativos,
                   SUM(cc.status_internet='AA') as aguard_ass,
                   COUNT(DISTINCT DATE(cc.data)) as dias_com_venda
            FROM ixcprovedor.cliente_contrato cc
            WHERE cc.id_vendedor_ativ=%s
              AND cc.data>=%s AND cc.data<=%s
        """, (vid, de, _ate))
        kpi = cur.fetchone()

        # OS por tipo
        cur.execute("""
            SELECT o.id_assunto, o.status AS os_status, COUNT(*) as total
            FROM ixcprovedor.su_oss_chamado o
            JOIN ixcprovedor.cliente_contrato cc ON o.id_contrato_kit=cc.id
            WHERE cc.id_vendedor_ativ=%s
              AND cc.data>=%s AND cc.data<=%s AND o.id_assunto IN (227,110,75,15)
            GROUP BY o.id_assunto, o.status
        """, (vid, de, _ate))
        os_rows = cur.fetchall()

        nv_inst = nv_pend = tit = rn = reativ = 0
        for r in os_rows:
            if r["id_assunto"]==227 and r["os_status"]=="F": nv_inst += r["total"]
            if r["id_assunto"]==227 and r["os_status"]=="A": nv_pend += r["total"]
            if r["id_assunto"]==110: tit += r["total"]
            if r["id_assunto"]==75:  rn  += r["total"]
            if r["id_assunto"]==15:  reativ += r["total"]

        # Adimplencia
        cur.execute("""
            SELECT COUNT(DISTINCT cc.id) as total,
                   COUNT(DISTINCT CASE WHEN f.status='A' AND f.data_vencimento < CURDATE() THEN cc.id END) as inadimplentes,
                   COUNT(DISTINCT CASE WHEN f.status='A' AND DATEDIFF(CURDATE(),f.data_vencimento)>30 THEN cc.id END) as criticos
            FROM ixcprovedor.cliente_contrato cc
            LEFT JOIN ixcprovedor.fn_areceber f ON f.id_contrato=cc.id
            WHERE cc.id_vendedor_ativ=%s
              AND cc.data>=%s AND cc.data<=%s AND cc.status='A'
        """, (vid, de, _ate))
        adim = cur.fetchone()

        # Vendas por dia (ultimos 30 dias para grafico)
        cur.execute("""
            SELECT DATE(cc.data) as dia, COUNT(*) as total
            FROM ixcprovedor.cliente_contrato cc
            WHERE cc.id_vendedor_ativ=%s
              AND cc.data>=%s AND cc.data<=%s AND cc.status='A'
            GROUP BY DATE(cc.data) ORDER BY dia
        """, (vid, de, _ate))
        por_dia = [{"dia": str(r["dia"]), "total": r["total"]} for r in cur.fetchall()]

        total = int(kpi["total"] or 0)
        dias  = int(kpi["dias_com_venda"] or 1)
        media_dia = round(total/dias, 1)

        def sv(v):
            if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
            return int(v) if v is not None else 0

        return {
            "vendedor": dict(vend) if vend else {},
            "kpi": {
                "total": total,
                "ativos": sv(kpi["ativos"]),
                "aguard_ass": sv(kpi["aguard_ass"]),
                "dias_com_venda": dias,
                "media_dia": media_dia,
            },
            "os": {
                "nv_instaladas": nv_inst,
                "nv_pendentes": nv_pend,
                "titularidade": tit,
                "reconexao": rn,
                "reativacao": reativ,
            },
            "adimplencia": {
                "total": sv(adim["total"]),
                "inadimplentes": sv(adim["inadimplentes"]),
                "criticos": sv(adim["criticos"]),
                "pct_adimplente": round((sv(adim["total"])-sv(adim["inadimplentes"]))/max(sv(adim["total"]),1)*100),
            },
            "por_dia": por_dia,
        }


@router.get("/vendedores/{vid}/perfil")
async def vendedor_perfil(
    vid: int,
    user=Depends(requer_backoffice())
):
    from datetime import date, timedelta
    from app.services.ixc_db import ixc_conn
    hoje = date.today().strftime("%Y-%m-%d")
    d90  = (date.today()-timedelta(days=90)).strftime("%Y-%m-%d")
    d2026 = "2026-01-01"

    with ixc_conn() as conn:
        cur = conn.cursor()

        cur.execute("SELECT id, nome FROM ixcprovedor.vendedor WHERE id=%s", (vid,))
        vend = cur.fetchone()

        # META: media de vendas/dia (meta = 4/dia)
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT DATE(data)) as dias
            FROM ixcprovedor.cliente_contrato
            WHERE id_vendedor_ativ=%s AND data>=%s AND status='A'
        """, (vid, d2026))
        r = cur.fetchone()
        total_vendas = int(r["total"] or 0)
        dias_venda   = int(r["dias"] or 1)
        media_dia    = round(total_vendas/dias_venda, 1)
        # Score meta: 4/dia = ideal
        if media_dia >= 4:    score_meta = 100; nivel_meta = "ideal"
        elif media_dia >= 2:  score_meta = 60;  nivel_meta = "medio"
        else:                 score_meta = 30;  nivel_meta = "baixo"

        # RETENCAO: clientes que completaram 90 dias e ainda estao ativos
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(status_internet='A') as retidos
            FROM ixcprovedor.cliente_contrato
            WHERE id_vendedor_ativ=%s
              AND data>=%s AND data<=%s AND status='A'
        """, (vid, d2026, d90))
        r = cur.fetchone()
        total_90  = int(r["total"] or 0)
        retidos   = int(r["retidos"] or 0)
        pct_ret   = round(retidos/max(total_90,1)*100)
        if pct_ret >= 90:   score_ret = 100; nivel_ret = "ideal"
        elif pct_ret >= 70: score_ret = 60;  nivel_ret = "medio"
        else:               score_ret = 30;  nivel_ret = "baixo"

        # ADIMPLENCIA: clientes dos ultimos 90 dias com faturas em dia
        cur.execute("""
            SELECT COUNT(DISTINCT cc.id) as total,
                   COUNT(DISTINCT CASE WHEN f.status='A' AND f.data_vencimento<CURDATE() THEN cc.id END) as inad
            FROM ixcprovedor.cliente_contrato cc
            LEFT JOIN ixcprovedor.fn_areceber f ON f.id_contrato=cc.id
            WHERE cc.id_vendedor_ativ=%s AND cc.data>=%s AND cc.status='A'
        """, (vid, d90))
        r = cur.fetchone()
        total_adim = int(r["total"] or 0)
        inad       = int(r["inad"] or 0)
        pct_adim   = round((total_adim-inad)/max(total_adim,1)*100)
        if pct_adim >= 90:   score_adim = 100; nivel_adim = "ideal"
        elif pct_adim >= 70: score_adim = 60;  nivel_adim = "medio"
        else:                score_adim = 30;  nivel_adim = "baixo"

        # SCORE FINAL
        score_final = round((score_meta + score_ret + score_adim) / 3)
        if score_final >= 80:   perfil = "Excelente"
        elif score_final >= 60: perfil = "Bom"
        elif score_final >= 40: perfil = "Regular"
        else:                   perfil = "Necessita atencao"

        return {
            "vendedor": dict(vend) if vend else {},
            "score_final": score_final,
            "perfil": perfil,
            "dimensoes": {
                "meta": {
                    "score": score_meta, "nivel": nivel_meta,
                    "media_dia": media_dia, "meta_dia": 4,
                    "total_vendas": total_vendas, "dias_ativos": dias_venda,
                    "descricao": "Media de vendas por dia ativo (meta: 4/dia)"
                },
                "retencao": {
                    "score": score_ret, "nivel": nivel_ret,
                    "pct": pct_ret, "retidos": retidos, "total": total_90,
                    "descricao": "Clientes que permaneceram 90+ dias na base"
                },
                "adimplencia": {
                    "score": score_adim, "nivel": nivel_adim,
                    "pct": pct_adim, "inadimplentes": inad, "total": total_adim,
                    "descricao": "Clientes com faturas em dia (ultimos 90 dias)"
                }
            }
        }


@router.get("/vendedores/{vid}/detalhe")
async def vendedor_detalhe(
    vid: int,
    tipo: str = Query(""),  # nv_inst, nv_pend, tit, rn, reativ, inadimplentes, criticos
    de: str = Query("2026-01-01"),
    ate: str = Query(""),
    user=Depends(requer_backoffice())
):
    from datetime import date
    from app.services.ixc_db import ixc_conn
    _ate = ate or date.today().strftime("%Y-%m-%d")

    with ixc_conn() as conn:
        cur = conn.cursor()

        if tipo in ("nv_inst","nv_pend","tit","rn","reativ"):
            assunto_map = {"nv_inst":227,"nv_pend":227,"tit":110,"rn":75,"reativ":15}
            status_map  = {"nv_inst":"F","nv_pend":"A","tit":"F","rn":"F","reativ":"F"}
            assunto = assunto_map[tipo]
            os_status = status_map[tipo]
            cur.execute("""
                SELECT cc.id AS contrato_id, c.razao, c.cnpj_cpf, c.telefone_celular,
                       ci.nome AS cidade, c.bairro,
                       vc.nome AS plano,
                       cc.data_ativacao,
                       o.data_abertura, o.data_fechamento,
                       DATEDIFF(COALESCE(o.data_fechamento,NOW()), o.data_abertura) AS sla,
                       f.funcionario AS tecnico
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                JOIN ixcprovedor.su_oss_chamado o ON o.id_contrato_kit=cc.id
                    AND o.id_assunto=%s AND o.status=%s
                LEFT JOIN ixcprovedor.funcionarios f ON f.id=o.id_tecnico
                WHERE cc.id_vendedor_ativ=%s
                  AND cc.data>=%s AND cc.data<=%s
                  AND cc.status='A'
                ORDER BY o.data_abertura DESC
            """, (assunto, os_status, vid, de, _ate))

        elif tipo in ("inadimplentes","criticos"):
            dias_min = 30 if tipo=="criticos" else 1
            cur.execute(f"""
                SELECT cc.id AS contrato_id, c.razao, c.cnpj_cpf, c.telefone_celular,
                       ci.nome AS cidade,
                       vc.nome AS plano,
                       MAX(DATEDIFF(CURDATE(),f.data_vencimento)) AS dias_atraso,
                       SUM(f.valor_aberto) AS valor_total
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                JOIN ixcprovedor.fn_areceber f ON f.id_contrato=cc.id
                    AND f.status='A' AND f.data_vencimento < CURDATE()
                WHERE cc.id_vendedor_ativ=%s
                  AND cc.data>=%s AND cc.status='A'
                GROUP BY cc.id HAVING MAX(DATEDIFF(CURDATE(),f.data_vencimento)) >= {dias_min}
                ORDER BY dias_atraso DESC
            """, (vid, de))
        else:
            return {"itens": []}

        rows = cur.fetchall()

    def sv(v):
        if v is None: return None
        if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
        if hasattr(v,'isoformat'): return str(v)
        return v

    return {"itens": [{k:sv(val) for k,val in dict(r).items()} for r in rows]}


# ── SYNC IXC MANUAL (botão no painel) ────────────────────────
@router.post("/sync-ixc")
async def sync_ixc_manual(user=Depends(requer_backoffice())):
    """Dispara sincronização manual de contratos e planos do IXC."""
    import subprocess
    base = str(Path(__file__).resolve().parent.parent.parent)
    try:
        subprocess.Popen(
            ["venv/bin/python", "-m", "app.bootstrap.cron_sync_contratos"],
            cwd=base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        subprocess.Popen(
            ["venv/bin/python", "-m", "app.bootstrap.cron_sync_planos_vendedores"],
            cwd=base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return {"ok": True, "msg": "Sync iniciado em background."}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


# ── RESUMO TV — dados direto do IXC para o painel TV ─────────
@router.get("/resumo-tv")
async def resumo_tv(user=Depends(requer_backoffice())):
    """
    KPIs para o Painel TV — dados direto do IXC.
    Total leads = contratos criados hoje.
    Ativados = status_internet='A' hoje.
    Aguard. Assinatura = status_internet='AA' hoje.
    """
    from datetime import date
    hoje = date.today().strftime("%Y-%m-%d")
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(cc.status_internet='A')  AS ativados,
                    SUM(cc.status_internet='AA') AS aguard_assinatura,
                    SUM(cc.status='P')           AS pendentes
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
                WHERE cc.data >= %s
                  AND cc.id_vendedor_ativ > 0
                  AND cc.id_vendedor_ativ != 29
            """, (hoje,))
            t = cur.fetchone()

            # Últimas ativações do dia
            cur.execute("""
                SELECT c.razao, v.nome AS vendedor,
                       cc.data AS data_cadastro,
                       cc.status_internet,
                       o.id_assunto AS tipo_os,
                       f.funcionario AS tecnico,
                       o.data_fechamento AS data_instalacao
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id = cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.su_oss_chamado o
                    ON o.id_contrato_kit = cc.id
                    AND o.id_assunto IN (227,110,75,15)
                LEFT JOIN ixcprovedor.funcionarios f ON f.id = o.id_tecnico
                WHERE cc.data >= %s
                  AND cc.id_vendedor_ativ > 0
                  AND cc.id_vendedor_ativ != 29
                ORDER BY cc.id DESC LIMIT 15
            """, (hoje,))
            atividades = cur.fetchall()

        def sv(v):
            if v is None: return None
            if hasattr(v, 'isoformat'): return str(v)
            if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal': return float(v)
            return v

        def fmt_row(r):
            d = {}
            for k, val in dict(r).items():
                d[k] = sv(val)
            # Garantir data_cadastro como string YYYY-MM-DD para o JS
            if d.get('data_cadastro') and not isinstance(d['data_cadastro'], str):
                d['data_cadastro'] = str(d['data_cadastro'])[:10]
            if d.get('data_instalacao') and not isinstance(d['data_instalacao'], str):
                d['data_instalacao'] = str(d['data_instalacao'])[:10]
            return d

        return {
            "totais": {
                "total":            int(t["total"] or 0),
                "ativados":         int(t["ativados"] or 0),
                "aguard_assinatura":int(t["aguard_assinatura"] or 0),
                "pendentes":        int(t["pendentes"] or 0),
                "reprovados":       0,
            },
            "atividades": [fmt_row(r) for r in atividades],
        }
    except Exception as e:
        log.error(f"resumo-tv: {e}")
        return {"totais": {"total":0,"ativados":0,"aguard_assinatura":0,"pendentes":0,"reprovados":0}, "atividades": []}


# ── ENDPOINT TESTE FOTO ───────────────────────────────────────
from fastapi import UploadFile, File as FastAPIFile
@router.post("/teste-foto", include_in_schema=False)
async def teste_foto(foto: UploadFile = FastAPIFile(...)):
    import io
    from PIL import Image as _Img
    from pathlib import Path
    raw = await foto.read()
    img = _Img.open(io.BytesIO(raw)).convert('RGB')
    MAX = 1200
    w, h = img.size
    if w > MAX or h > MAX:
        ratio = min(MAX/w, MAX/h)
        img = img.resize((int(w*ratio), int(h*ratio)), _Img.LANCZOS)
    dest = Path("/opt/automacoes/cliquedf/comercial/uploads/teste_foto.jpg")
    dest.parent.mkdir(exist_ok=True)
    img.save(str(dest), 'JPEG', quality=75)
    kb = dest.stat().st_size // 1024
    return {"ok": True, "width": img.width, "height": img.height,
            "kb": kb, "url": "/uploads/teste_foto.jpg"}



# ── OPA Suite: Monitor de Atendimentos ──────────────────────────
import httpx

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'

@router.get('/opa/atendimentos')
async def opa_atendimentos(data: str = None):
    from datetime import date
    import json
    if not data:
        data = str(date.today())
    payload = {"filter": {"dataInicialAbertura": data, "dataFinalAbertura": data}, "options": {"limit": 500}}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.request(
                method='GET',
                url=f'{OPA_BASE}/atendimento',
                headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
                content=json.dumps(payload).encode()
            )
        return r.json()
    except Exception as e:
        return {'status': 'error', 'message': str(e), 'data': []}

@router.get('/opa/fila')
async def opa_fila():
    import json
    from datetime import date
    hoje = str(date.today())
    payload = {"filter": {"status": "AG"}, "options": {"limit": 100}}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.request(
                method='GET',
                url=f'{OPA_BASE}/atendimento',
                headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
                content=json.dumps(payload).encode()
            )
        data = r.json()
        # Filtrar apenas os de hoje
        atends = [a for a in data.get('data', []) if a.get('date','')[:10] == hoje]
        return {'status': 'success', 'data': atends}
    except Exception as e:
        return {'status': 'error', 'message': str(e), 'data': []}

@router.get('/opa/cliente-nome')
async def opa_cliente_nome(tel: str = ''):
    from app.services.ixc_db import ixc_select
    if not tel or len(tel) < 8:
        return {'nome': '?'}
    try:
        digits = ''.join(c for c in tel if c.isdigit())
        suffix = digits[-9:]
        r = ixc_select(
            "SELECT razao FROM cliente WHERE telefone_celular LIKE %s OR fone LIKE %s OR whatsapp LIKE %s LIMIT 1",
            (f'%{suffix}', f'%{suffix}', f'%{suffix}')
        )
        nome = r[0]['razao'] if r else '?'
        return {'nome': nome}
    except:
        return {'nome': '?'}

@router.get('/opa/atendimento/{atend_id}')
async def opa_atendimento_detalhe(atend_id: str):
    import json
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f'{OPA_BASE}/atendimento/{atend_id}',
                headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'}
            )
        return r.json()
    except Exception as e:
        return {'status': 'error', 'message': str(e), 'data': {}}

@router.post('/opa/clientes-nomes')
async def opa_clientes_nomes(body: dict):
    from app.services.ixc_db import ixc_conn
    tels = body.get('tels', [])
    if not tels:
        return {'nomes': {}}
    try:
        tel_map = {}
        for tel in tels:
            digits = ''.join(c for c in tel if c.isdigit())
            # Remover prefixo 55 do Brasil
            if digits.startswith('55') and len(digits) >= 12:
                digits = digits[2:]
            # Formatar igual IXC: (79) 99901-0781
            if len(digits) == 11:  # DDD + 9 digitos
                fmt = f'({digits[:2]}) {digits[2:7]}-{digits[7:]}'
            elif len(digits) == 10:  # DDD + 8 digitos
                fmt = f'({digits[:2]}) {digits[2:6]}-{digits[6:]}'
            else:
                fmt = digits
            tel_map[tel] = fmt
        if not tel_map:
            return {'nomes': {}}
        # Busca exata pelo telefone formatado
        formatos = list(set(tel_map.values()))
        conds = ' OR '.join(['telefone_celular = %s OR fone = %s OR whatsapp = %s'] * len(formatos))
        params = []
        for f in formatos:
            params += [f, f, f]
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute('SELECT razao, telefone_celular, fone, whatsapp FROM cliente WHERE ' + conds, params)
            rows = cur.fetchall()
        fmt_nome = {}
        for row in rows:
            for campo in ['telefone_celular', 'fone', 'whatsapp']:
                val = row.get(campo) or ''
                if val and val not in fmt_nome:
                    fmt_nome[val] = row['razao']
        resultado = {tel: fmt_nome.get(fmt, '?') for tel, fmt in tel_map.items()}
        return {'nomes': resultado}
    except Exception as e:
        return {'nomes': {}}


@router.post('/opa/historico-os')
async def opa_historico_os(body: dict):
    from app.services.ixc_db import ixc_conn
    tels = body.get('tels', [])
    if not tels:
        return {'historico': {}}
    try:
        tel_map = {}
        for tel in tels:
            digits = ''.join(c for c in tel if c.isdigit())
            # Remover prefixo 55 do Brasil
            if digits.startswith('55') and len(digits) >= 12:
                digits = digits[2:]
            # Formatar igual IXC: (79) 99901-0781
            if len(digits) == 11:  # DDD + 9 digitos
                fmt = f'({digits[:2]}) {digits[2:7]}-{digits[7:]}'
            elif len(digits) == 10:  # DDD + 8 digitos
                fmt = f'({digits[:2]}) {digits[2:6]}-{digits[6:]}'
            else:
                fmt = digits
            tel_map[tel] = fmt

        if not tel_map:
            return {'historico': {}}

        # Busca exata pelo telefone formatado
        formatos = list(set(tel_map.values()))
        conds = ' OR '.join(['c.telefone_celular = %s OR c.fone = %s OR c.whatsapp = %s'] * len(formatos))
        params = []
        for f in formatos:
            params += [f, f, f]

        with ixc_conn() as conn:
            cur = conn.cursor()
            # Construir query sem f-string para evitar conflito com % do pymysql
            sql = (
                "SELECT o.id, o.id_assunto, o.status, o.data_abertura, o.data_fechamento,"
                " c.telefone_celular, c.fone, c.whatsapp, f.funcionario as tecnico,"
                " o.mensagem as obs_abertura,"
                " CASE o.id_assunto"
                " WHEN 16 THEN 'Manutencao'"
                " WHEN 20 THEN 'Sem acesso'"
                " WHEN 21 THEN 'Internet lenta'"
                " ELSE 'Outro' END as assunto,"
                " (SELECT m.mensagem FROM su_oss_chamado_mensagem m"
                "  WHERE m.id_chamado = o.id AND m.status = 'F'"
                "  ORDER BY m.data DESC LIMIT 1) as obs_fechamento"
                " FROM su_oss_chamado o"
                " JOIN cliente c ON c.id = o.id_cliente"
                " LEFT JOIN funcionarios f ON f.id = o.id_tecnico"
                " WHERE o.id_assunto IN (16, 20, 21)"
                " AND o.data_abertura >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)"
                " AND (" + conds + ")"
                " ORDER BY o.data_abertura DESC LIMIT 50"
            )
            cur.execute(sql, params)
            rows = cur.fetchall()

        # Agrupar por telefone formatado
        fmt_os = {}
        for row in rows:
            entry = {
                'id': row['id'],
                'assunto': row['assunto'],
                'status': row['status'],
                'data_abertura': str(row['data_abertura']) if row['data_abertura'] else None,
                'data_fechamento': str(row['data_fechamento']) if row['data_fechamento'] else None,
                'tecnico': row['tecnico'],
                'obs_abertura': (row.get('obs_abertura') or '')[:200],
                'obs_fechamento': (row.get('obs_fechamento') or '')[:300],
            }
            for campo in ['telefone_celular', 'fone', 'whatsapp']:
                val = row.get(campo) or ''
                if val:
                    if val not in fmt_os:
                        fmt_os[val] = []
                    if entry not in fmt_os[val]:
                        fmt_os[val].append(entry)

        resultado = {}
        for tel, fmt in tel_map.items():
            os_list = fmt_os.get(fmt) or []
            # Deduplicar por ID
            seen = set()
            dedup = []
            for o in os_list:
                if o['id'] not in seen:
                    seen.add(o['id'])
                    dedup.append(o)
            resultado[tel] = dedup

        return {'historico': resultado}
    except Exception as e:
        return {'historico': {}, 'erro': str(e)}

# ── WEBHOOK OPA ───────────────────────────────────────────────
from fastapi import Request as _Request
import sqlite3 as _sq3, os as _os
from datetime import datetime as _dt, timezone as _tz, timedelta as _td

_NOMES_ATD_WH = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '67602d9691afc2bf7a36ed6c': 'Leide Aquino',
    '5d1642ad4b16a50312cc8f4d': 'Caique (bot)',
}
_DEPTOS_WH = {
    '5bf73d1d186f7d2b0d647a61': 'Suporte',
    '5bf73d1d186f7d2b0d647a60': 'Comercial',
    '5d1624085e74a002308aa25e': 'Financeiro',
    '5bf73d1d186f7d2b0d647a64': 'Ag. Virtual',
    '5d1629315e74a002308aa262': 'Renegociacoes',
}

@router.post('/opa/webhook')
async def opa_webhook(req: _Request):
    BRT = _tz(_td(hours=-3))
    agora = _dt.now(BRT).strftime('%Y-%m-%d %H:%M:%S')
    try:
        body = await req.json()
    except:
        return {'status': 'ok'}

    event = body.get('event', {})
    event_type = event.get('type', '')
    data = event.get('data', {})

    # Ignorar verificacao
    if event_type == 'verification':
        return {'status': 'ok'}

    # Extrair dados do atendimento
    atend_id  = data.get('_id', '')
    protocolo = data.get('protocol', '')
    canal_cli = (data.get('customerChannel') or '').replace('@c.us', '')
    id_atd    = data.get('attendantId', '')
    nome_atd  = _NOMES_ATD_WH.get(id_atd, '')
    depto_id  = data.get('departmentId', '')
    depto     = _DEPTOS_WH.get(depto_id, depto_id[:8] if depto_id else '')
    status    = data.get('status', '')

    # Payload da acao (customerServiceEvent)
    payload = data.get('payload', {})
    if payload:
        atend_id  = atend_id  or payload.get('_id', '')
        protocolo = protocolo or payload.get('protocol', '')
        canal_cli = canal_cli or (payload.get('customerChannel') or '').replace('@c.us', '')
        id_atd    = id_atd    or payload.get('attendantId', '')
        nome_atd  = nome_atd  or _NOMES_ATD_WH.get(id_atd, '')
        depto_id  = depto_id  or payload.get('departmentId', '')
        depto     = depto     or _DEPTOS_WH.get(depto_id, '')
        status    = status    or payload.get('status', '')

    if not atend_id:
        return {'status': 'ok'}

    # Salvar no SQLite
    db = _os.path.join(_os.path.dirname(__file__), '../../hub_comercial.db')
    conn = _sq3.connect(_os.path.abspath(db))
    try:
        conn.execute('''INSERT INTO opa_atendimentos
            (atend_id,protocolo,canal_cliente,id_atendente,nome_atendente,setor,status,atualizado_em)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(atend_id) DO UPDATE SET
            status=excluded.status,
            id_atendente=COALESCE(NULLIF(excluded.id_atendente,\'\'),id_atendente),
            nome_atendente=COALESCE(NULLIF(excluded.nome_atendente,\'\'),nome_atendente),
            atualizado_em=excluded.atualizado_em''',
            (atend_id,protocolo,canal_cli,id_atd,nome_atd,depto,status,agora))

        # Interpretar evento com descrição clara
        if event_type == 'waitingForCustomerResponse':
            descricao = f'📵 Cliente parou de responder | Atendente: {nome_atd or "?"}'
        elif event_type == 'customerServiceEvent':
            if status == 'EA' and nome_atd:
                descricao = f'👤 {nome_atd} assumiu o atendimento'
            elif status == 'AG' and not nome_atd:
                descricao = '🔴 Cliente entrou na fila — sem atendente'
            elif status == 'F':
                descricao = f'✅ Atendimento finalizado por {nome_atd or "?"}'
            elif status == 'AG' and nome_atd:
                descricao = f'⏳ {nome_atd} aguardando cliente'
            else:
                descricao = f'📋 {event_type} | status={status} | {nome_atd}'
        else:
            descricao = f'{event_type} | status={status} | {nome_atd}'
        conn.execute('''INSERT INTO opa_mensagens
            (atend_id,protocolo,canal_cliente,remetente,tipo,mensagem,data_hora,criado_em)
            VALUES (?,?,?,?,?,?,?,?)''',
            (atend_id,protocolo,canal_cli,'sistema',event_type,descricao,agora,agora))
        conn.commit()
    finally:
        conn.close()

    # Finalizar ticket IXC se atendimento foi finalizado no Opa
    if status == 'F' and protocolo:
        _finalizar_ticket_ixc(protocolo)

    return {'status': 'ok'}

@router.get('/opa/timeline/{atend_id}')
async def opa_timeline(atend_id: str):
    import sqlite3 as sq, os
    db = os.path.join(os.path.dirname(__file__), '../../hub_comercial.db')
    conn = sq.connect(os.path.abspath(db))
    conn.row_factory = sq.Row
    try:
        eventos = conn.execute(
            'SELECT tipo, mensagem, data_hora FROM opa_mensagens WHERE atend_id=? ORDER BY data_hora',
            (atend_id,)
        ).fetchall()
        atend = conn.execute(
            'SELECT * FROM opa_atendimentos WHERE atend_id=?', (atend_id,)
        ).fetchone()
        return {
            'atendimento': dict(atend) if atend else {},
            'timeline': [dict(e) for e in eventos]
        }
    finally:
        conn.close()

@router.post('/opa/aguardando')
async def opa_aguardando(body: dict):
    """Retorna quem está aguardando em cada atendimento baseado nos eventos do webhook"""
    import sqlite3 as sq, os
    atend_ids = body.get('ids', [])
    if not atend_ids:
        return {'aguardando': {}}
    db = os.path.join(os.path.dirname(__file__), '../../hub_comercial.db')
    conn = sq.connect(os.path.abspath(db))
    conn.row_factory = sq.Row
    resultado = {}
    ATENDENTES = ['Amanda','Karine','Johnatan','Manuela','Rudinedja','Leide']
    try:
        for atend_id in atend_ids:
            # Buscar por atend_id ou protocolo
            row = conn.execute(
                'SELECT tipo, mensagem FROM opa_mensagens WHERE atend_id=? OR protocolo=? ORDER BY data_hora DESC LIMIT 1',
                (atend_id, atend_id)
            ).fetchone()
            if not row:
                resultado[atend_id] = {'quem': 'atendente', 'label': '👤 Atendente'}
            elif row['tipo'] == 'waitingForCustomerResponse':
                resultado[atend_id] = {'quem': 'cliente', 'label': '📵 Cliente'}
            elif 'assumiu' in (row['mensagem'] or ''):
                resultado[atend_id] = {'quem': 'cliente', 'label': '📵 Cliente'}
            elif any(n in (row['mensagem'] or '') for n in ATENDENTES):
                resultado[atend_id] = {'quem': 'cliente', 'label': '📵 Cliente'}
            else:
                resultado[atend_id] = {'quem': 'atendente', 'label': '👤 Atendente'}
    finally:
        conn.close()
    return {'aguardando': resultado}

# ── Finalizar ticket IXC quando Opa finaliza ─────────────────
def _finalizar_ticket_ixc(protocolo_opa: str):
    from app.services.ixc_db import ixc_conn as _conn
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM su_ticket WHERE titulo LIKE %s AND status = 'T' LIMIT 1",
                (f'%{protocolo_opa}%',)
            )
            ticket = cur.fetchone()
            if not ticket:
                return False
            cur.execute(
                "UPDATE su_ticket SET status='F', ultima_atualizacao=NOW() WHERE id=%s",
                (ticket['id'],)
            )
            conn.commit()
            print(f'[IXC] Ticket finalizado para {protocolo_opa}')
            return True
    except Exception as e:
        print(f'[IXC] Erro: {e}')
        return False


# ── ALTERAÇÃO DE PLANOS POR VENCIMENTO ─────────────────────────────────────
@router.get("/retencao/meses")
def retencao_meses(user=Depends(requer_backoffice())):
    from app.services.ixc_db import ixc_conn
    with ixc_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DATE_FORMAT(cc.data_expiracao, '%Y-%m') AS mes, COUNT(*) AS total
            FROM ixcprovedor.cliente_contrato cc
            WHERE cc.status = 'A'
              AND cc.data_expiracao >= DATE_FORMAT(NOW() - INTERVAL 2 MONTH, '%Y-%m-01')
              AND cc.data_expiracao <= DATE_FORMAT(NOW() + INTERVAL 6 MONTH, '%Y-%m-28')
            GROUP BY mes ORDER BY mes ASC
        """)
        rows = cur.fetchall()
    meses_pt = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
    result = []
    for r in rows:
        mes = r['mes']; ano, m = mes.split('-')
        result.append({"mes": mes, "label": f"{meses_pt[int(m)]}/{ano}", "total": r['total']})
    return result


@router.get("/retencao")
def retencao_contratos(mes: str = "", user=Depends(requer_backoffice())):
    from app.services.ixc_db import ixc_conn
    import sqlite3 as _sq, calendar as _cal
    from app.routes.retencao import DB_PATH
    from datetime import date
    if not mes:
        mes = date.today().strftime("%Y-%m")
    ano, m = mes.split('-')
    data_ini = f"{ano}-{m}-01"
    ultimo = _cal.monthrange(int(ano), int(m))[1]
    data_fim = f"{ano}-{m}-{ultimo:02d}"
    with ixc_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT cc.id AS contrato_id, cl.id AS cliente_id, cl.razao AS cliente,
                   vd.nome AS plano_nome, vd.valor_contrato AS plano_valor,
                   cc.data_expiracao, cl.telefone_celular AS telefone,
                   ci.nome AS cidade_nome
            FROM ixcprovedor.cliente_contrato cc
            JOIN  ixcprovedor.cliente cl      ON cl.id  = cc.id_cliente
            JOIN  ixcprovedor.vd_contratos vd ON vd.id  = cc.id_vd_contrato
            LEFT  JOIN ixcprovedor.cidade ci  ON ci.id  = cl.cidade
            WHERE cc.status = 'A'
              AND cc.data_expiracao >= %s AND cc.data_expiracao <= %s
            ORDER BY cc.data_expiracao ASC, cl.razao ASC
        """, (data_ini, data_fim))
        rows = cur.fetchall()
    acoes = {}
    try:
        con2 = _sq.connect(DB_PATH)
        con2.row_factory = _sq.Row
        for row in con2.execute("SELECT ixc_contrato_id, status_retencao, obs, responsavel FROM hc_retencao_acoes").fetchall():
            acoes[row['ixc_contrato_id']] = dict(row)
        con2.close()
    except Exception:
        pass
    contratos = []
    kpis = {"total": 0, "receita": 0.0, "pendentes": 0, "em_contato": 0, "retidos": 0, "cancelados": 0}
    for r in rows:
        cid = r['contrato_id']; acao = acoes.get(cid, {}); status = acao.get('status_retencao', 'nao_contatado'); valor = float(r['plano_valor'] or 0)
        contratos.append({"contrato_id": cid, "cliente_id": r['cliente_id'], "cliente": r['cliente'], "plano_nome": r['plano_nome'], "plano_valor": valor, "data_expiracao": str(r['data_expiracao']), "telefone": r['telefone'], "cidade_nome": r['cidade_nome'], "status_retencao": status, "obs": acao.get('obs',''), "responsavel": acao.get('responsavel','')})
        kpis['total'] += 1; kpis['receita'] += valor
        if status == 'nao_contatado': kpis['pendentes'] += 1
        elif status in ('em_contato','negociando','confirmado'): kpis['em_contato'] += 1
        elif status == 'alterado': kpis['retidos'] += 1
        elif status == 'recusou': kpis['cancelados'] += 1
    return {"contratos": contratos, "kpis": kpis}
