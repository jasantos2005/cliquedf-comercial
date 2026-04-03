import logging
from app.services.ixc_db import ixc_conn
log = logging.getLogger(__name__)

REGRAS = [
    {"id":"R01","legenda":"OS instalacao aberta +7 dias","nivel":"critico"},
    {"id":"R02","legenda":"OS instalacao aberta +3 dias","nivel":"alerta"},
    {"id":"R03","legenda":"Inadimplencia critica (+30d)","nivel":"critico"},
    {"id":"R04","legenda":"Atraso grave (16-30 dias)","nivel":"grave"},
    {"id":"R05","legenda":"Atraso leve (1-15 dias)","nivel":"alerta"},
    {"id":"R06","legenda":"Sem email cadastrado","nivel":"alerta"},
    {"id":"R07","legenda":"Sem data de nascimento","nivel":"alerta"},
    {"id":"R08","legenda":"Sem celular cadastrado","nivel":"alerta"},
    {"id":"R09","legenda":"Pre-contrato ha mais de 30 dias","nivel":"grave"},
]
NIVEL_ORDER = {"critico":0,"grave":1,"alerta":2,"ok":3}

def _sv(v):
    if v is None: return None
    if hasattr(v,'__class__') and v.__class__.__name__=='Decimal': return float(v)
    if hasattr(v,'isoformat'): return str(v)
    return v

def auditar_contratos(de="2026-01-01", ate=None, vendedor_id="", cidade=""):
    from datetime import date
    if not ate: ate = date.today().strftime("%Y-%m-%d")
    resultados = {}
    filtro_vend = f"AND cc.id_vendedor_ativ = {int(vendedor_id)}" if vendedor_id else ""
    filtro_cid  = f"AND c.cidade = {int(cidade)}" if cidade and str(cidade).isdigit() else ""
    bw = f"WHERE cc.data >= '{de}' AND cc.data <= '{ate}' AND cc.id_vendedor_ativ > 0 AND cc.id_vendedor_ativ != 29 {filtro_vend} {filtro_cid}"

    def upsert(r):
        cid = r["contrato_id"]
        if cid not in resultados:
            resultados[cid] = {
                "contrato_id": cid,
                "razao": r.get("razao",""),
                "cnpj_cpf": r.get("cnpj_cpf",""),
                "vendedor_nome": r.get("vendedor_nome",""),
                "cidade_nome": r.get("cidade_nome",""),
                "plano_nome": r.get("plano_nome",""),
                "data_contrato": str(r.get("data_contrato","")),
                "status_contrato": r.get("status_contrato",""),
                "status_internet": r.get("status_internet",""),
                "problemas": [],
                "nivel_max": "ok",
            }
        return resultados[cid]

    def add(r, rid, detalhe=""):
        ct = upsert(r)
        rg = next((x for x in REGRAS if x["id"]==rid), None)
        if not rg: return
        ct["problemas"].append({"regra":rid,"legenda":rg["legenda"],"nivel":rg["nivel"],"detalhe":detalhe})
        if NIVEL_ORDER.get(rg["nivel"],3) < NIVEL_ORDER.get(ct["nivel_max"],3):
            ct["nivel_max"] = rg["nivel"]

    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            sel_base = f"""SELECT cc.id AS contrato_id, cc.data AS data_contrato,
                cc.status AS status_contrato, cc.status_internet,
                c.razao, c.cnpj_cpf, c.email, c.telefone_celular, c.data_nascimento,
                v.nome AS vendedor_nome, ci.nome AS cidade_nome, vc.nome AS plano_nome
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id=cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                {bw}"""

            # R01/R02 OS aberta
            cur.execute(f"""SELECT cc.id AS contrato_id, cc.data AS data_contrato,
                cc.status AS status_contrato, cc.status_internet,
                c.razao, c.cnpj_cpf, c.email, c.telefone_celular, c.data_nascimento,
                v.nome AS vendedor_nome, ci.nome AS cidade_nome, vc.nome AS plano_nome,
                DATEDIFF(NOW(), o.data_abertura) AS dias
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id=cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                JOIN ixcprovedor.su_oss_chamado o ON o.id_contrato_kit=cc.id
                    AND o.id_assunto=227 AND o.status='A'
                {bw}""")
            for r in cur.fetchall():
                d = int(r["dias"] or 0)
                if d > 7: add(r,"R01",f"{d} dias sem instalar")
                elif d > 3: add(r,"R02",f"{d} dias sem instalar")

            # R03/R04/R05 Inadimplencia
            cur.execute(f"""SELECT cc.id AS contrato_id, cc.data AS data_contrato,
                cc.status AS status_contrato, cc.status_internet,
                c.razao, c.cnpj_cpf, c.email, c.telefone_celular, c.data_nascimento,
                v.nome AS vendedor_nome, ci.nome AS cidade_nome, vc.nome AS plano_nome,
                MAX(DATEDIFF(NOW(), f.data_vencimento)) AS max_atraso,
                SUM(f.valor_aberto) AS valor_aberto
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id=cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                JOIN ixcprovedor.fn_areceber f ON f.id_contrato=cc.id
                    AND f.status='A' AND f.data_vencimento < CURDATE()
                {bw}
                GROUP BY cc.id""")
            for r in cur.fetchall():
                d = int(r["max_atraso"] or 0)
                vl = float(r["valor_aberto"] or 0)
                det = f"{d}d — R$ {vl:.2f}"
                if d > 30: add(r,"R03",det)
                elif d > 15: add(r,"R04",det)
                elif d > 0: add(r,"R05",det)

            # R06/R07/R08 Dados cadastrais
            cur.execute(sel_base)
            for r in cur.fetchall():
                if not r.get("email"): add(r,"R06","Email vazio")
                nasc = r.get("data_nascimento")
                if not nasc or str(nasc) in ("None","0000-00-00",""): add(r,"R07","Sem data nasc")
                if not r.get("telefone_celular"): add(r,"R08","Sem celular")

            # R09 Pre-contrato antigo
            cur.execute(f"""SELECT cc.id AS contrato_id, cc.data AS data_contrato,
                cc.status AS status_contrato, cc.status_internet,
                c.razao, c.cnpj_cpf, c.email, c.telefone_celular, c.data_nascimento,
                v.nome AS vendedor_nome, ci.nome AS cidade_nome, vc.nome AS plano_nome,
                DATEDIFF(NOW(), cc.data) AS dias_pre
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id=cc.id_cliente
                LEFT JOIN ixcprovedor.vendedor v ON v.id=cc.id_vendedor_ativ
                LEFT JOIN ixcprovedor.cidade ci ON ci.id=c.cidade
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id=cc.id_vd_contrato
                {bw} AND cc.status='P'""")
            for r in cur.fetchall():
                d = int(r["dias_pre"] or 0)
                if d > 30: add(r,"R09",f"{d} dias como pre-contrato")

    except Exception as e:
        log.error(f"auditoria_ixc_engine: {e}")

    lista = [v for v in resultados.values() if v["problemas"]]
    lista.sort(key=lambda x: NIVEL_ORDER.get(x["nivel_max"],3))
    return lista

def resumo_auditoria(de="2026-01-01", ate=None):
    lista = auditar_contratos(de, ate)
    por_regra = {}
    for ct in lista:
        for p in ct["problemas"]:
            k = p["regra"]
            if k not in por_regra:
                por_regra[k] = {"regra":k,"legenda":p["legenda"],"nivel":p["nivel"],"total":0}
            por_regra[k]["total"] += 1
    return {
        "total_problemas": len(lista),
        "por_nivel": {
            "critico": sum(1 for c in lista if c["nivel_max"]=="critico"),
            "grave":   sum(1 for c in lista if c["nivel_max"]=="grave"),
            "alerta":  sum(1 for c in lista if c["nivel_max"]=="alerta"),
        },
        "por_regra": sorted(por_regra.values(), key=lambda x: x["total"], reverse=True),
    }
