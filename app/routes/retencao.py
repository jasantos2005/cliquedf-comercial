"""
routes/retencao.py — HubRetenção Cliquedf
IaTechHub · Ailton
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import sqlite3, os, json
from datetime import datetime
from app.services.ixc_db import ixc_select, ixc_select_one

router = APIRouter(prefix="/api/retencao", tags=["retencao"])
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "hub_comercial.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_retencao_tables():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS hc_churn_score (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_contrato_id INTEGER NOT NULL,
        ixc_cliente_id INTEGER, cliente_nome TEXT, cpf TEXT,
        cidade TEXT, bairro TEXT, plano_nome TEXT, plano_valor REAL,
        status_contrato TEXT, data_ativacao TEXT,
        score INTEGER DEFAULT 0, faixa TEXT,
        pts_financeiro INTEGER DEFAULT 0, pts_tecnico INTEGER DEFAULT 0,
        pts_comportamental INTEGER DEFAULT 0, pts_contextual INTEGER DEFAULT 0,
        motivos TEXT, script_sugerido TEXT, calculado_em TEXT,
        UNIQUE(ixc_contrato_id)
    );
    CREATE TABLE IF NOT EXISTS hc_retencao_acoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_contrato_id INTEGER NOT NULL, cliente_nome TEXT,
        score_momento INTEGER, operador_id INTEGER, operador_nome TEXT,
        resultado TEXT, obs TEXT,
        criado_em TEXT DEFAULT (datetime('now','-3 hours'))
    );
    """)
    conn.commit()
    conn.close()

def calcular_score_contrato(contrato_id: int) -> dict:
    motivos = []
    pts_fin = pts_tec = pts_comp = pts_ctx = 0

    row = ixc_select_one("""
        SELECT cc.id, cc.id_cliente, cc.status, cc.data_ativacao,
               cc.id_vd_contrato AS id_plano, cc.valor_unitario AS valor_plano, cc.valor_unitario AS plano_valor,
               cc.fidelidade AS data_fidelidade,
               cl.razao AS nome, cl.cnpj_cpf AS cpf,
               cc.cidade, cc.bairro
        FROM cliente_contrato cc
        JOIN cliente cl ON cl.id = cc.id_cliente
        WHERE cc.id = %s
    """, (contrato_id,))
    if not row:
        return None

    plano_row = ixc_select_one("SELECT nome FROM vd_contratos WHERE id = %s", (row["id_plano"],))
    plano_nome = plano_row["nome"] if plano_row else "Desconhecido"

    meses_casa = 0
    try:
        data_ativ = datetime.strptime(str(row["data_ativacao"])[:10], "%Y-%m-%d")
        meses_casa = (datetime.now() - data_ativ).days // 30
    except Exception:
        pass

    fat = ixc_select_one("""
        SELECT
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) > 15 THEN 1 ELSE 0 END) AS grave,
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) BETWEEN 5 AND 15 THEN 1 ELSE 0 END) AS moderado,
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS leve
        FROM fn_areceber
        WHERE id_contrato = %s AND status IN ('A','P') AND data_vencimento < CURDATE()
    """, (contrato_id,))
    if fat:
        if fat["grave"] and int(fat["grave"]) > 0:
            pts_fin += 35
            motivos.append(f"💰 {fat['grave']} fatura(s) vencida(s) há mais de 15 dias")
        elif fat["moderado"] and int(fat["moderado"]) > 0:
            pts_fin += 20
            motivos.append("💰 Fatura vencida há 5-15 dias")
        elif fat["leve"] and int(fat["leve"]) > 0:
            pts_fin += 10
            motivos.append("💰 Fatura vencida nos últimos 4 dias")

    susp = ixc_select_one("""
        SELECT COUNT(*) AS total FROM cliente_contrato_historico
        WHERE id_contrato = %s AND tipo = 'S'
          AND data >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
    """, (contrato_id,))
    susp_total = int(susp["total"]) if susp and susp["total"] else 0
    if susp_total >= 3:
        pts_fin += 15
        motivos.append(f"💰 {susp_total} suspensoes nos ultimos 6 meses")
    elif susp_total >= 1:
        pts_fin += 7
        motivos.append(f"💰 {susp_total} suspensao(oes) nos ultimos 6 meses")

    os_ant = ixc_select_one("""
        SELECT COUNT(*) AS total FROM su_oss_chamado
        WHERE id_contrato_kit = %s AND status = 'A'
          AND DATEDIFF(CURDATE(), data_abertura) > 7
    """, (contrato_id,))
    if os_ant and int(os_ant["total"] or 0) > 0:
        pts_tec += 30
        motivos.append("🔧 OS aberta ha mais de 7 dias sem resolucao")

    os_mes = ixc_select_one("""
        SELECT COUNT(*) AS total FROM su_oss_chamado
        WHERE id_contrato_kit = %s
          AND data_abertura >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    """, (contrato_id,))
    os_mes_total = int(os_mes["total"] or 0) if os_mes else 0
    if os_mes_total >= 3:
        pts_tec += 20
        motivos.append(f"🔧 {os_mes_total} OS abertas este mes (recorrencia tecnica)")
    elif os_mes_total == 2:
        pts_tec += 10
        motivos.append("🔧 2 OS abertas este mes")

    reab = ixc_select_one("""
        SELECT COUNT(*) AS total FROM su_oss_chamado o1
        WHERE o1.id_contrato_kit = %s AND o1.status = 'F'
          AND o1.data_fechamento >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
          AND EXISTS (
              SELECT 1 FROM su_oss_chamado o2
              WHERE o2.id_contrato_kit = %s
                AND o2.data_abertura > o1.data_fechamento
                AND DATEDIFF(o2.data_abertura, o1.data_fechamento) <= 5
          )
    """, (contrato_id, contrato_id))
    if reab and int(reab["total"] or 0) > 0:
        pts_tec += 20
        motivos.append("🔧 OS reaberta no mes (problema nao resolvido na 1a vez)")

    if row["data_fidelidade"]:
        try:
            df = datetime.strptime(str(row["data_fidelidade"])[:10], "%Y-%m-%d")
            dias_fim = (df - datetime.now()).days
            if 0 < dias_fim <= 30:
                pts_comp += 20
                motivos.append(f"⏰ Fidelidade expira em {dias_fim} dias")
            elif 0 < dias_fim <= 60:
                pts_comp += 12
                motivos.append(f"⏰ Fidelidade expira em {dias_fim} dias")
        except Exception:
            pass

    os_12m = ixc_select_one("""
        SELECT COUNT(*) AS total FROM su_oss_chamado
        WHERE id_contrato_kit = %s
          AND data_abertura >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
    """, (contrato_id,))
    if os_12m and int(os_12m["total"] or 0) == 0 and meses_casa > 12:
        pts_comp += 10
        motivos.append("👻 Cliente sem nenhuma interacao nos ultimos 12 meses")

    if datetime.now().month in (1, 2):
        pts_ctx += 8
        motivos.append("📅 Periodo sazonal de alto cancelamento (Jan/Fev)")

    bonus = 0
    if meses_casa > 24:
        bonus = -10
        motivos.append("✅ Cliente fiel ha mais de 2 anos (-10 pts)")
    elif meses_casa > 12:
        bonus = -5

    pts_fin  = min(pts_fin, 35)
    pts_tec  = min(pts_tec, 30)
    pts_comp = min(pts_comp, 20)
    pts_ctx  = min(pts_ctx, 15)
    score = max(0, min(100, pts_fin + pts_tec + pts_comp + pts_ctx + bonus))
    faixa = "alto" if score >= 40 else ("medio" if score >= 20 else "baixo")

    if not [m for m in motivos if not m.startswith("✅")]:
        script = "✅ Cliente sem fatores de risco. Ligacao de relacionamento."
    elif pts_fin >= pts_tec:
        script = "💰 Foco financeiro: ofereça negociacao de debito ou desconto pontual."
    elif pts_tec > pts_comp:
        script = "🔧 Foco tecnico: comprometa-se com resolucao definitiva. Ofereça tecnico prioritario."
    elif pts_comp >= 15:
        script = "⏰ Fidelidade proxima do fim: ofereça renovacao com beneficios."
    else:
        script = "📋 Retencao preventiva: escute o cliente e ofereça beneficio surpresa."


    # Dados de conexao
    rad = ixc_select_one(
        "SELECT id, login, online, ultima_conexao_inicial, ultima_conexao_final,"
        " tempo_conectado, count_desconexao, motivo_desconexao, download_atual, upload_atual"
        " FROM radusuarios WHERE id_contrato = %s LIMIT 1",
        (contrato_id,)
    )
    consumo = None
    if rad:
        consumo = ixc_select_one(
            "SELECT SUM(consumo) AS total_down, SUM(consumo_upload) AS total_up"
            " FROM radusuarios_consumo"
            " WHERE id_login = %s AND data >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)",
            (rad['id'],)
        )

    ultimas_os = ixc_select("""
        SELECT o.id, o.data_abertura, o.data_fechamento, o.status,
               a.assunto AS assunto_texto,
               o.mensagem_resposta AS solucao,
               f.funcionario AS tecnico_nome
        FROM su_oss_chamado o
        LEFT JOIN su_oss_assunto a ON a.id = o.id_assunto
        LEFT JOIN funcionarios f ON f.id = o.id_tecnico
        WHERE o.id_contrato_kit = %s
          AND o.id_assunto IN (20, 21, 16, 94, 113, 248)
        ORDER BY o.data_abertura DESC LIMIT 10
    """, (contrato_id,))

    faturas = ixc_select("""
        SELECT id, data_vencimento, valor, status
        FROM fn_areceber
        WHERE id_contrato = %s AND status IN ('A','P')
        ORDER BY data_vencimento ASC LIMIT 5
    """, (contrato_id,))

    return {
        "ixc_contrato_id": contrato_id,
        "ixc_cliente_id":  row["id_cliente"],
        "cliente_nome":    row["nome"],
        "cpf":             row["cpf"],
        "cidade":          row["cidade"] or "",
        "bairro":          row["bairro"] or "",
        "plano_nome":      plano_nome,
        "plano_valor":     float(row["plano_valor"] or 0),
        "status_contrato": row["status"],
        "data_ativacao":   str(row["data_ativacao"])[:10] if row["data_ativacao"] else "",
        "meses_casa":      meses_casa,
        "score":           score,
        "faixa":           faixa,
        "pts_financeiro":  pts_fin,
        "pts_tecnico":     pts_tec,
        "pts_comportamental": pts_comp,
        "pts_contextual":  pts_ctx,
        "motivos":         motivos,
        "script_sugerido": script,
        "ultimas_os":      [dict(o) for o in ultimas_os],
        "conexao": {
            "login":             rad["login"] if rad else None,
            "online":            rad["online"] == "S" if rad else False,
            "ultima_conexao":    str(rad["ultima_conexao_inicial"])[:16] if rad and rad["ultima_conexao_inicial"] else None,
            "ultima_desconexao": str(rad["ultima_conexao_final"])[:16] if rad and rad["ultima_conexao_final"] else None,
            "tempo_conectado":   rad["tempo_conectado"] if rad else None,
            "quedas":            int(rad["count_desconexao"] or 0) if rad else 0,
            "motivo_desconexao": rad["motivo_desconexao"] if rad else None,
            "download_atual":    rad["download_atual"] if rad else None,
            "upload_atual":      rad["upload_atual"] if rad else None,
            "consumo_down_30d":  int(consumo["total_down"] or 0) if consumo else 0,
            "consumo_up_30d":    int(consumo["total_up"] or 0) if consumo else 0,
        },
        "faturas_abertas": [dict(f) for f in faturas],
        "conexao": {
            "login":             rad["login"] if rad else None,
            "online":            rad["online"] == "S" if rad else False,
            "ultima_conexao":    str(rad["ultima_conexao_inicial"])[:16] if rad and rad["ultima_conexao_inicial"] else None,
            "ultima_desconexao": str(rad["ultima_conexao_final"])[:16] if rad and rad["ultima_conexao_final"] else None,
            "tempo_conectado":   rad["tempo_conectado"] if rad else None,
            "quedas":            int(rad["count_desconexao"] or 0) if rad else 0,
            "motivo_desconexao": rad["motivo_desconexao"] if rad else None,
            "consumo_down_30d":  int(consumo["total_down"] or 0) if consumo else 0,
            "consumo_up_30d":    int(consumo["total_up"] or 0) if consumo else 0,
        },
        "calculado_em":    datetime.now().strftime("%d/%m/%Y %H:%M"),
    }

def _salvar_cache(ficha: dict):
    db = get_db()
    db.execute("""
        INSERT INTO hc_churn_score
            (ixc_contrato_id, ixc_cliente_id, cliente_nome, cpf, cidade, bairro,
             plano_nome, plano_valor, status_contrato, data_ativacao,
             score, faixa, pts_financeiro, pts_tecnico, pts_comportamental, pts_contextual,
             motivos, script_sugerido, calculado_em)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(ixc_contrato_id) DO UPDATE SET
            score=excluded.score, faixa=excluded.faixa,
            pts_financeiro=excluded.pts_financeiro, pts_tecnico=excluded.pts_tecnico,
            pts_comportamental=excluded.pts_comportamental, pts_contextual=excluded.pts_contextual,
            motivos=excluded.motivos, script_sugerido=excluded.script_sugerido,
            calculado_em=excluded.calculado_em,
            plano_nome=excluded.plano_nome, status_contrato=excluded.status_contrato
    """, (
        ficha["ixc_contrato_id"], ficha["ixc_cliente_id"], ficha["cliente_nome"],
        ficha["cpf"], ficha["cidade"], ficha["bairro"],
        ficha["plano_nome"], ficha["plano_valor"], ficha["status_contrato"],
        ficha["data_ativacao"], ficha["score"], ficha["faixa"],
        ficha["pts_financeiro"], ficha["pts_tecnico"],
        ficha["pts_comportamental"], ficha["pts_contextual"],
        json.dumps(ficha["motivos"], ensure_ascii=False),
        ficha["script_sugerido"], ficha["calculado_em"]
    ))
    db.commit()
    db.close()

@router.get("/buscar")
def buscar_cliente(q: str):
    q = q.strip()
    cpf_limpo = "".join(filter(str.isdigit, q))
    if q.isdigit() and len(q) <= 7:
        contratos = ixc_select("""
            SELECT cc.id AS contrato_id, cl.razao AS nome, cl.cnpj_cpf AS cpf
            FROM cliente_contrato cc JOIN cliente cl ON cl.id = cc.id_cliente
            WHERE cc.id = %s AND cc.status = 'A'
        """, (int(q),))
    elif len(cpf_limpo) in (11, 14):
        contratos = ixc_select("""
            SELECT cc.id AS contrato_id, cl.razao AS nome, cl.cnpj_cpf AS cpf
            FROM cliente_contrato cc JOIN cliente cl ON cl.id = cc.id_cliente
            WHERE REPLACE(REPLACE(REPLACE(cl.cnpj_cpf,'.',''),'-',''),'/','') = %s
              AND cc.status = 'A'
        """, (cpf_limpo,))
    else:
        contratos = ixc_select("""
            SELECT cc.id AS contrato_id, cl.razao AS nome, cl.cnpj_cpf AS cpf
            FROM cliente_contrato cc JOIN cliente cl ON cl.id = cc.id_cliente
            WHERE cl.razao LIKE %s AND cc.status = 'A'
            LIMIT 10
        """, (f"%{q}%",))
    if not contratos:
        raise HTTPException(404, "Nenhum contrato ativo encontrado")
    if len(contratos) > 1:
        return {"tipo": "lista", "opcoes": [{"contrato_id": c["contrato_id"], "nome": c["nome"], "cpf": c["cpf"]} for c in contratos]}
    contrato_id = contratos[0]["contrato_id"]
    db = get_db()
    cache = db.execute("SELECT * FROM hc_churn_score WHERE ixc_contrato_id = ?", (contrato_id,)).fetchone()
    db.close()
    if cache and cache["calculado_em"]:
        try:
            calc_em = datetime.strptime(cache["calculado_em"], "%d/%m/%Y %H:%M")
            if (datetime.now() - calc_em).total_seconds() < 86400:
                # conexao sempre em tempo real (nao salva no cache)
                _cx_rad = ixc_select_one(
                    "SELECT id, login, online, ultima_conexao_inicial, ultima_conexao_final,"
                    " tempo_conectado, count_desconexao, motivo_desconexao"
                    " FROM radusuarios WHERE id_contrato = %s LIMIT 1",
                    (contrato_id,)
                )
                _cx_consumo = None
                if _cx_rad:
                    _cx_consumo = ixc_select_one(
                        "SELECT SUM(consumo) AS total_down, SUM(consumo_upload) AS total_up"
                        " FROM radusuarios_consumo"
                        " WHERE id_login = %s AND data >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)",
                        (_cx_rad['id'],)
                    )
                _conexao_live = {
                    "login":             _cx_rad["login"] if _cx_rad else None,
                    "online":            _cx_rad["online"] == "S" if _cx_rad else False,
                    "ultima_conexao":    str(_cx_rad["ultima_conexao_inicial"])[:16] if _cx_rad and _cx_rad["ultima_conexao_inicial"] else None,
                    "ultima_desconexao": str(_cx_rad["ultima_conexao_final"])[:16] if _cx_rad and _cx_rad["ultima_conexao_final"] else None,
                    "tempo_conectado":   _cx_rad["tempo_conectado"] if _cx_rad else None,
                    "quedas":            int(_cx_rad["count_desconexao"] or 0) if _cx_rad else 0,
                    "motivo_desconexao": _cx_rad["motivo_desconexao"] if _cx_rad else None,
                    "consumo_down_30d":  int(_cx_consumo["total_down"] or 0) if _cx_consumo else 0,
                    "consumo_up_30d":    int(_cx_consumo["total_up"] or 0) if _cx_consumo else 0,
                } if _cx_rad else None
                return {"tipo": "ficha", "fonte": "cache", **dict(cache),
                        "conexao": _conexao_live,
                        "motivos": json.loads(cache["motivos"] or "[]"),
                        "ultimas_os": [], "faturas_abertas": []}
        except Exception:
            pass
    ficha = calcular_score_contrato(contrato_id)
    if not ficha:
        raise HTTPException(404, "Contrato nao encontrado no IXC")
    _salvar_cache(ficha)
    return {"tipo": "ficha", "fonte": "tempo_real", **ficha}

@router.get("/ficha/{contrato_id}")
def ficha_contrato(contrato_id: int):
    ficha = calcular_score_contrato(contrato_id)
    if not ficha:
        raise HTTPException(404, "Contrato nao encontrado")
    _salvar_cache(ficha)
    return ficha

@router.get("/fila")
def fila_retencao(faixa: str = "alto"):
    db = get_db()
    if faixa == "todos":
        rows = db.execute("SELECT * FROM hc_churn_score WHERE faixa IN ('alto','medio') ORDER BY score DESC LIMIT 200").fetchall()
    else:
        rows = db.execute("SELECT * FROM hc_churn_score WHERE faixa = ? ORDER BY score DESC LIMIT 200", (faixa,)).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        d["motivos"] = json.loads(d.get("motivos") or "[]")
        result.append(d)
    return result

@router.get("/stats")
def stats_retencao():
    db = get_db()
    stats = db.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN faixa='alto' THEN 1 ELSE 0 END) as alto_risco,
               SUM(CASE WHEN faixa='medio' THEN 1 ELSE 0 END) as medio_risco,
               SUM(CASE WHEN faixa='baixo' THEN 1 ELSE 0 END) as baixo_risco,
               AVG(score) as score_medio
        FROM hc_churn_score
    """).fetchone()
    acoes_mes = db.execute("""
        SELECT resultado, COUNT(*) as total FROM hc_retencao_acoes
        WHERE criado_em >= datetime('now','-3 hours','-30 days')
        GROUP BY resultado
    """).fetchall()
    db.close()
    return {"totais": dict(stats) if stats else {}, "acoes_mes": [dict(a) for a in acoes_mes]}

class AcaoInput(BaseModel):
    ixc_contrato_id: int
    cliente_nome: str
    score_momento: int
    resultado: str
    obs: Optional[str] = ""

@router.post("/acao")
def registrar_acao(data: AcaoInput):
    db = get_db()
    db.execute("""
        INSERT INTO hc_retencao_acoes
            (ixc_contrato_id, cliente_nome, score_momento, operador_id, operador_nome, resultado, obs)
        VALUES (?,?,?,?,?,?,?)
    """, (data.ixc_contrato_id, data.cliente_nome, data.score_momento,
          usuario["id"], usuario["nome"], data.resultado, data.obs))
    db.commit()
    db.close()
    return {"ok": True}

from app.services.ixc_db import ixc_insert
from datetime import datetime as _dt

class AbrirOSInput(BaseModel):
    ixc_contrato_id: int
    ixc_cliente_id: int
    id_assunto: int
    mensagem: Optional[str] = ""

@router.post("/abrir-os")
def abrir_os(data: AbrirOSInput):
    agora = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = """
        INSERT INTO ixcprovedor.su_oss_chamado (
            id_cliente, id_contrato_kit, id_assunto, status, id_filial,
            data_abertura, mensagem, id_tecnico, setor,
            origem_cadastro, ultima_atualizacao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        data.ixc_cliente_id, data.ixc_contrato_id, data.id_assunto,
        "A", 1, agora, data.mensagem, 0, "27", "W", agora
    )
    os_id = ixc_insert(sql, params)
    if not os_id:
        raise HTTPException(500, "Falha ao gerar OS no IXC")
    return {"ok": True, "os_id": os_id}
