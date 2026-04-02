"""
Hub Comercial — app/engines/contrato_engine.py
Gera o HTML do contrato substituindo as 54 variáveis do modelo IXC.
"""
import re, uuid, logging
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
from app.services.ixc_db import ixc_select_one

log = logging.getLogger(__name__)

MODELO_ID  = 13
FILIAL_ID  = 1
EXPIRA_H   = 48

FILIAL = None

def _get_filial():
    global FILIAL
    if FILIAL: return FILIAL
    FILIAL = ixc_select_one("SELECT * FROM ixcprovedor.filial WHERE id=%s", (FILIAL_ID,))
    return FILIAL

def _cidade_nome(cidade_id):
    try:
        r = ixc_select_one("SELECT nome,uf FROM ixcprovedor.cidade WHERE id=%s", (cidade_id,))
        return (r["nome"] if r else ""), (r["uf"] if r else "")
    except: return "", ""

def gerar_html_contrato(precadastro: dict) -> str:
    modelo = ixc_select_one(
        "SELECT texto, cabecalho, prazo, fidelidade FROM ixcprovedor.cliente_contrato_modelo WHERE id=%s",
        (MODELO_ID,)
    )
    if not modelo:
        return "<p>Modelo de contrato não encontrado.</p>"

    f = _get_filial() or {}
    p = precadastro

    cidade_nome, cidade_uf = "", ""
    if p.get("ixc_cidade_id"):
        cidade_nome, cidade_uf = _cidade_nome(p["ixc_cidade_id"])
    if not cidade_nome:
        cidade_nome = p.get("cidade_nome") or ""
    if not cidade_uf:
        cidade_uf = p.get("uf_sigla") or ""

    plano_r = ixc_select_one(
        "SELECT nome, valor_contrato FROM ixcprovedor.vd_contratos WHERE id=%s",
        (p.get("ixc_plano_id"),)
    ) if p.get("ixc_plano_id") else None

    from decimal import Decimal
    def fmt_val(v):
        if v is None: return "0,00"
        return f"{float(v):.2f}".replace(".", ",")

    variaveis = {
        # Cliente
        "cliente_razao":              p.get("razao") or "",
        "cliente_fantasia":           p.get("razao") or "",
        "cliente_CNPJ_CPF":           p.get("cnpj_cpf") or "",
        "cliente_RG_IE":              p.get("ie_identidade") or "",
        "cliente_rg_orgao_emissor":   "",
        "cliente_celular":            p.get("telefone_celular") or "",
        "cliente_fone":               p.get("telefone_celular") or "",
        "cliente_email":              p.get("email") or "",
        "cliente_uf":                 cidade_uf,
        "cliente_tipo_assinante":     "Pessoa Física" if (p.get("tipo_pessoa") or "F").upper()=="F" else "Pessoa Jurídica",
        "cliente_nome_representante_1":   p.get("razao") or "",
        "cliente_cpf_representante_1":    p.get("cnpj_cpf") or "",
        "cliente_identidade_representante_1": "",
        "cliente_inscricao_municipal":    "",
        # Contrato / endereço
        "contrato_endereco":          p.get("endereco") or "",
        "contrato_endereco_numero":   p.get("numero") or "",
        "contrato_bairro":            p.get("bairro") or "",
        "contrato_cidade":            cidade_nome,
        "contrato_estado":            cidade_uf,
        "contrato_cep":               p.get("cep") or "",
        "contrato_complemento":       p.get("complemento") or "",
        "contrato_bloco":             "",
        "contrato_apartamento":       "",
        "contrato_condominio":        "",
        "contrato_numero":            "",
        "contrato_dia_vencimento":    str(p.get("dia_vencimento") or ""),
        "contrato_fidelidade_meses":  str(p.get("fidelidade") or "12"),
        "contrato_taxa_instalacao":   fmt_val(p.get("taxa_instalacao")),
        "cliente_contrato_taxa_instalacao": fmt_val(p.get("taxa_instalacao")),
        "cliente_contrato_taxa_improdutiva": "0,00",
        "contrato_data_ativacao":     datetime.now().strftime("%d/%m/%Y"),
        "contrato_forma_cobranca":    "Boleto",
        "contrato_desc_aux_plano_venda": p.get("plano_nome") or "",
        "contrato_grade_comodato_2":  "",
        # Plano / velocidade
        "download":                   (p.get("plano_nome") or "").split()[0] if p.get("plano_nome") else "",
        "upload":                     "",
        "valor_total_produtos":       fmt_val(p.get("plano_valor")),
        "grade_produtos_sem_desconto_e_acrescimo": p.get("plano_nome") or "",
        # Filial
        "filial_razao":               f.get("razao") or "",
        "filial_fantasia":            f.get("fantasia") or "",
        "filial_cnpj":                f.get("cnpj") or "",
        "filial_endereco":            f.get("endereco") or "",
        "filial_numero":              str(f.get("numero") or ""),
        "filial_bairro":              f.get("bairro") or "",
        "filial_cep":                 f.get("cep") or "",
        "filial_cidade":              str(f.get("cidade") or ""),
        "filial_uf":                  "",
        "filial_complemento":         f.get("complemento") or "",
        "filial_telefone":            f.get("telefone") or "",
        "filial_telefone1":           f.get("telefone1") or "",
        "filial_email":               f.get("email") or "",
        "filial_site":                f.get("site") or "",
        "filial_ato_anatel":          f.get("ato_anatel") or "",
        # Assinaturas (preenchidas após assinar)
        "assinatura_cliente":         "___________________________",
        "assinatura_empresa":         "___________________________",
    }

    texto = (modelo["cabecalho"] or "") + "<hr>" + (modelo["texto"] or "")
    if modelo.get("prazo"):
        texto += "<br><br>" + modelo["prazo"]
    if modelo.get("fidelidade"):
        texto += "<br><br>" + modelo["fidelidade"]

    # Garante que todos os valores são string
    variaveis = {k: str(v) if v is not None else "" for k, v in variaveis.items()}

    def substituir(m):
        return variaveis.get(m.group(1), m.group(0))

    return re.sub(r"#(\w+)#", substituir, texto)


def gerar_token_assinatura(precadastro_id: int) -> dict:
    token   = uuid.uuid4().hex
    expira  = (datetime.now() + timedelta(hours=EXPIRA_H)).strftime("%Y-%m-%d %H:%M:%S")
    return {"token": token, "expira_em": expira}
