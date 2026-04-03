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
                WHERE cc.data >= %s AND cc.status != 'C'
                  AND cc.id_vendedor_ativ > 0 AND cc.id_vendedor_ativ != 29
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
        WHERE cc.data >= %s AND cc.data <= %s
          AND cc.id_vendedor_ativ > 0 AND cc.id_vendedor_ativ != 29
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
                  AND cc.id_vendedor_ativ > 0 AND cc.id_vendedor_ativ != 29
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
    user=Depends(requer_backoffice())
):
    from app.engines.auditoria_ixc_engine import auditar_contratos
    lista = auditar_contratos(de, ate or None, vendedor_id, cidade)
    if nivel:
        lista = [c for c in lista if c["nivel_max"] == nivel]
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
