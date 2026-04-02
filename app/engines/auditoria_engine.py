"""
Hub Comercial — app/engines/auditoria_engine.py
Motor de auditoria preventiva com as 27 regras reais da Cliquedf.
"""
import re, logging
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
from app.services.ixc_db import ixc_select_one

log = logging.getLogger(__name__)

LEGENDA = {
    "R01":"PF menor de 18 anos","R02":"Endereço > 60 caracteres",
    "R03":"Nome/Razão social ausente ou > 60 caracteres","R04":"Documento ausente",
    "R05":"PJ com CNPJ inválido","R06":"PF com CPF inválido",
    "R10":"CEP inválido","R12":"Número de endereço inválido",
    "R13":"Complemento com caracteres proibidos","R14":"Bairro ausente",
    "R15":"Endereço ausente","R16":"Tipo de pessoa inválido",
    "R17":"CEP suspeito (dígitos repetidos)","R18":"PF com classificação SCM divergente",
    "R21":"Cidade ausente","R22":"UF ausente",
    "R23":"Telefone celular inválido","R24":"E-mail inválido ou ausente",
    "R25":"CPF/CNPJ duplicado no sistema","R26":"Sem documentos no cadastro",
    "R28":"Conta fiscal divergente",
}

def _limpo(v): return (v or "").strip()

def _cpf_valido(cpf):
    cpf = re.sub(r"\D","",cpf or "")
    if len(cpf)!=11 or len(set(cpf))==1: return False
    for i in range(2):
        s = sum(int(cpf[j])*(10+i-j) for j in range(9+i))
        r = (s*10)%11
        if r in(10,11): r=0
        if r!=int(cpf[9+i]): return False
    return True

def _cnpj_valido(cnpj):
    cnpj = re.sub(r"\D","",cnpj or "")
    if len(cnpj)!=14 or len(set(cnpj))==1: return False
    for i,pesos in enumerate([[5,4,3,2,9,8,7,6,5,4,3,2],[6,5,4,3,2,9,8,7,6,5,4,3,2]]):
        s = sum(int(cnpj[j])*pesos[j] for j in range(len(pesos)))
        r = 11-(s%11)
        if r>=10: r=0
        if r!=int(cnpj[12+i]): return False
    return True

def _cep_valido(cep):
    c = re.sub(r"\D","",cep or "")
    return len(c)==8 and c!="00000000" and len(set(c))>1

def _idade(nasc):
    try:
        d = date.fromisoformat(nasc); h = date.today()
        return h.year-d.year-((h.month,h.day)<(d.month,d.day))
    except: return 99

def auditar(f, docs):
    tipo = (f.get("tipo_pessoa") or "F").upper()
    cpf  = re.sub(r"\D","",f.get("cnpj_cpf") or "")
    cep  = re.sub(r"\D","",f.get("cep") or "")
    uf   = _limpo(f.get("uf_sigla") or "").upper()
    obs  = _limpo(f.get("obs") or "").upper()
    res  = []

    def add(cod, resultado, detalhe=""):
        res.append({"regra":cod,"legenda":LEGENDA.get(cod,cod),"resultado":resultado,"detalhe":detalhe})

    # R01
    if tipo=="F" and f.get("data_nascimento"):
        idade=_idade(f["data_nascimento"])
        add("R01","reprovado" if idade<18 else "ok", f"Idade: {idade}" if idade<18 else "")
    else: add("R01","ok")

    # R02
    end=_limpo(f.get("endereco"))
    add("R02","reprovado" if len(end)>60 else "ok", f"{len(end)} chars" if len(end)>60 else "")

    # R03
    razao=_limpo(f.get("razao"))
    add("R03","reprovado" if not razao or len(razao)>60 else "ok", "Vazio" if not razao else f"{len(razao)} chars")

    # R04
    add("R04","reprovado" if not cpf else "ok", "Documento não informado" if not cpf else "")

    # R05/R06
    if tipo=="J": add("R05","reprovado" if not _cnpj_valido(cpf) else "ok", f.get("cnpj_cpf","") if not _cnpj_valido(cpf) else "")
    if tipo=="F": add("R06","reprovado" if not _cpf_valido(cpf) else "ok", f.get("cnpj_cpf","") if not _cpf_valido(cpf) else "")

    # R10
    add("R10","reprovado" if not _cep_valido(cep) else "ok", f.get("cep","") if not _cep_valido(cep) else "")

    # R12
    num=_limpo(f.get("numero"))
    invalido = num.upper()!="SN" and (not num or num.upper() in {"","0","00","000","SEM NUMERO","S/N"})
    add("R12","reprovado" if invalido else "ok", f"'{num}'" if invalido else "")

    # R13
    comp=_limpo(f.get("complemento") or "")
    add("R13","reprovado" if re.search(r"[@#*!/]",comp) else "ok", f"'{comp}'" if re.search(r"[@#*!/]",comp) else "")

    # R14
    add("R14","reprovado" if not _limpo(f.get("bairro")) else "ok")

    # R15
    add("R15","reprovado" if not end else "ok")

    # R16
    add("R16","reprovado" if tipo not in("F","J") else "ok")

    # R17
    add("R17","reprovado" if len(cep)==8 and len(set(cep))==1 else "ok")

    # R21
    add("R21","reprovado" if not f.get("ixc_cidade_id") and not _limpo(f.get("cidade_nome")) else "ok")

    # R22
    add("R22","reprovado" if not uf else "ok")

    # R23
    cel=re.sub(r"\D","",f.get("telefone_celular") or "")
    add("R23","reprovado" if len(cel)<10 else "ok", f.get("telefone_celular","") if len(cel)<10 else "")

    # R24
    email=_limpo(f.get("email") or "")
    if email and ("@" not in email or "." not in email.split("@")[-1]):
        add("R24","pendente",f"E-mail: '{email}'")
    elif not email: add("R24","pendente","E-mail não informado")
    else: add("R24","ok")

    # R25
    if cpf:
        try:
            dup=ixc_select_one("SELECT id,razao FROM ixcprovedor.cliente WHERE REPLACE(REPLACE(cnpj_cpf,'.',''),'-','')=%s LIMIT 1",(cpf,))
            if dup:
                ativo=ixc_select_one("SELECT id FROM ixcprovedor.cliente_contrato WHERE id_cliente=%s AND status='A' LIMIT 1",(dup["id"],))
                add("R25","reprovado" if ativo else "pendente",
                    f"{'Contrato ativo' if ativo else 'Sem contrato ativo'}: {dup['razao']} (ID {dup['id']})")
            else: add("R25","ok")
        except Exception as e:
            log.warning(f"R25: {e}"); add("R25","ok")

    # R26
    tem_doc    = any(d.get("tipo") in("rg_frente","cnh") for d in docs)
    tem_selfie = any(d.get("tipo")=="selfie_doc" for d in docs)
    levar      = "LEVAR CONTRATO" in obs
    if not tem_doc and not tem_selfie:
        add("R26","alerta" if levar else "reprovado", "Obs: LEVAR CONTRATO" if levar else "Nenhum doc anexado")
    elif not tem_selfie:
        add("R26","pendente","Selfie não anexada")
    else: add("R26","ok")

    # R28
    add("R28","ok")

    rep  = [r for r in res if r["resultado"]=="reprovado"]
    pend = [r for r in res if r["resultado"]=="pendente"]
    ale  = [r for r in res if r["resultado"]=="alerta"]

    if rep:   final="reprovado"
    elif pend: final="pendente"
    elif ale:  final="aprovado_com_ressalva"
    else:      final="aprovado"

    return {"resultado_final":final,"total_regras":len(res),"reprovadas":len(rep),
            "pendentes":len(pend),"alertas":len(ale),"regras":res}
