"""
Hub Comercial — app/services/credito_service.py

Consulta de análise de crédito via API IXC.
Lógica Opção C:
  1. Verifica se já existe consulta recente no IXC (últimos N dias)
  2. Se existe → retorna os dados cached
  3. Se não existe → retorna orientação para consultar no IXC
"""
import os, requests, base64, logging, warnings
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
log = logging.getLogger(__name__)

IXC_URL   = os.getenv("IXC_API_URL", "")
IXC_USER  = os.getenv("IXC_API_USER", "")
IXC_TOKEN = os.getenv("IXC_API_TOKEN", "")
DIAS_CACHE = 30  # reusar consulta se feita nos últimos 30 dias


def _headers():
    cred = base64.b64encode(f"{IXC_USER}:{IXC_TOKEN}".encode()).decode()
    return {
        "Authorization": f"Basic {cred}",
        "ixcsoft": "listar",
        "Content-Type": "application/json",
    }


def _ixc_post(endpoint: str, payload: dict) -> dict:
    r = requests.post(
        f"{IXC_URL}/webservice/v1/{endpoint}",
        headers=_headers(), json=payload,
        timeout=15, verify=False
    )
    return r.json() if r.text.strip() else {}


def classificar_risco(ocorrencias: int, valor: float) -> dict:
    """Classifica o risco de crédito com base nas ocorrências e valor."""
    if ocorrencias == 0:
        return {"nivel": "baixo", "label": "Sem restrições", "cor": "green"}
    elif ocorrencias <= 2 and valor <= 500:
        return {"nivel": "medio", "label": "Restrições leves", "cor": "amber"}
    elif ocorrencias <= 5 and valor <= 2000:
        return {"nivel": "alto", "label": "Restrições moderadas", "cor": "orange"}
    else:
        return {"nivel": "critico", "label": "Restrições graves", "cor": "red"}


def consultar_cpf(cpf_cnpj: str) -> dict:
    """
    Consulta análise de crédito para um CPF/CNPJ.
    Retorna dict com status e dados da consulta.
    """
    cpf_limpo = cpf_cnpj.replace(".", "").replace("-", "").replace("/", "").strip()

    if not IXC_URL:
        return {"status": "erro", "msg": "API IXC não configurada."}

    # 1. Busca consulta recente no IXC
    data_limite = (datetime.now() - timedelta(days=DIAS_CACHE)).strftime("%Y-%m-%d")
    try:
        d = _ixc_post("consulta_spc_serasa", {
            "qtype": "consulta_spc_serasa.cnpj_cpf",
            "query": cpf_limpo,
            "oper": "=",
            "page": "1",
            "rp": "1",
            "sortname": "id",
            "sortorder": "desc",
        })
    except Exception as e:
        log.error(f"Erro ao consultar IXC: {e}")
        return {"status": "erro", "msg": "Erro ao conectar com o IXC."}

    registros = d.get("registros", [])

    # PJ: IXC pode gravar cnpj_cpf vazio — buscar por id_lead via tabela contatos
    if not registros:
        try:
            from app.services.ixc_db import ixc_select
            cpf_raw = cpf_cnpj.replace(".","").replace("-","").replace("/","").strip()
            contatos = ixc_select(
                "SELECT id FROM ixcprovedor.contato WHERE REPLACE(REPLACE(REPLACE(cnpj_cpf,\'.\',\'\'),\'-\',\'\'),\'/\',\'\')=%s LIMIT 1",
                (cpf_raw,)
            )
            if contatos:
                id_lead = contatos[0]["id"]
                rows = ixc_select(
                    "SELECT id, data_hora_consulta, total_ocorrencias, valor_total, intermediador, id_lead "
                    "FROM ixcprovedor.consulta_spc_serasa WHERE id_lead=%s ORDER BY id DESC LIMIT 1",
                    (id_lead,)
                )
                if rows:
                    registros = [{
                        "id": rows[0]["id"],
                        "data_hora_consulta": str(rows[0]["data_hora_consulta"]),
                        "total_ocorrencias": int(rows[0]["total_ocorrencias"] or 0),
                        "valor_total": float(rows[0]["valor_total"] or 0),
                        "intermediador": rows[0]["intermediador"],
                        "id_lead": rows[0]["id_lead"],
                    }]
        except Exception as e:
            log.warning(f"Busca PJ por contato falhou: {e}")

    if registros:
        reg = registros[0]
        data_consulta = reg.get("data_hora_consulta", "")[:10]
        ocorrencias   = int(reg.get("total_ocorrencias") or 0)
        valor_total   = float(reg.get("valor_total") or 0)
        risco         = classificar_risco(ocorrencias, valor_total)
        dias_atras    = (datetime.now() - datetime.strptime(data_consulta, "%Y-%m-%d")).days if data_consulta else 0

        return {
            "status":          "encontrado",
            "cache":           True,
            "dias_atras":      dias_atras,
            "id_consulta":     reg.get("id"),
            "cpf_cnpj":        cpf_cnpj,
            "data_consulta":   data_consulta,
            "ocorrencias":     ocorrencias,
            "valor_total":     valor_total,
            "intermediador":   reg.get("intermediador", "CREDITONM"),
            "risco":           risco,
            "link_ixc":        f"{IXC_URL}/index.php#tab_leads" if reg.get("id_lead") else f"{IXC_URL}/index.php#tab_clientes",
            "msg":             f"Consulta realizada há {dias_atras} dia(s)."
                               if dias_atras > 0 else "Consulta realizada hoje.",
        }

    # 2. Sem consulta prévia — orientar a consultar no IXC
    return {
        "status":    "nao_consultado",
        "cache":     False,
        "cpf_cnpj":  cpf_cnpj,
        "ocorrencias": None,
        "valor_total": None,
        "risco":     None,
        "link_ixc":  f"{IXC_URL}/index.php",
        "msg":       "Nenhuma consulta encontrada. Realize a consulta no IXC.",
    }
