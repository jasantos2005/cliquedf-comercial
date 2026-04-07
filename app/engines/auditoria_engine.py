"""
Hub Comercial — app/engines/auditoria_engine.py
================================================
Motor de auditoria preventiva com as 27 regras da Cliquedf.

Executado pelo cron_auditoria.py a cada 5 minutos sobre cadastros
com status='enviado'.

Resultado final possível:
    aprovado             → todas as regras OK (pode ter alertas)
    aprovado_com_ressalva → apenas alertas, sem reprovações/pendências
    pendente             → há regras pendentes (sem reprovações)
    reprovado            → há pelo menos uma regra reprovada

Regras implementadas:
    R01  PF menor de 18 anos
    R02  Endereço com mais de 60 caracteres
    R03  Nome/Razão social ausente ou maior que 60 caracteres
    R04  CPF/CNPJ ausente
    R05  PJ com CNPJ inválido
    R06  PF com CPF inválido
    R10  CEP inválido
    R12  Número de endereço inválido
    R13  Complemento com caracteres proibidos
    R14  Bairro ausente
    R15  Endereço ausente
    R16  Tipo de pessoa inválido
    R17  CEP suspeito (todos os dígitos iguais)
    R21  Cidade ausente
    R22  UF ausente
    R23  Telefone celular inválido
    R24  E-mail inválido ou ausente
    R25  CPF/CNPJ duplicado no IXC (com contrato ativo = reprovado; sem = pendente)
    R26  Documentos ausentes (RG obrigatório, selfie obrigatória)
    R27  Cliente com débito em aberto no IXC (alerta)
    R28  Conta fiscal divergente (sempre OK por enquanto)
"""
import re, logging
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
from app.services.ixc_db import ixc_select_one

log = logging.getLogger(__name__)

# ── Legendas das regras ───────────────────────────────────────
LEGENDA = {
    "R01": "PF menor de 18 anos",
    "R02": "Endereço > 60 caracteres",
    "R03": "Nome/Razão social ausente ou > 60 caracteres",
    "R04": "Documento ausente",
    "R05": "PJ com CNPJ inválido",
    "R06": "PF com CPF inválido",
    "R10": "CEP inválido",
    "R12": "Número de endereço inválido",
    "R13": "Complemento com caracteres proibidos",
    "R14": "Bairro ausente",
    "R15": "Endereço ausente",
    "R16": "Tipo de pessoa inválido",
    "R17": "CEP suspeito (dígitos repetidos)",
    "R21": "Cidade ausente",
    "R22": "UF ausente",
    "R23": "Telefone celular inválido",
    "R24": "E-mail inválido ou ausente",
    "R25": "CPF/CNPJ duplicado no sistema",
    "R26": "Sem documentos no cadastro",
    "R27": "Cliente com débito em aberto no IXC",
    "R28": "Conta fiscal divergente",
}


# ── Funções auxiliares de validação ──────────────────────────

def _limpo(v: str) -> str:
    return (v or "").strip()


def _cpf_valido(cpf: str) -> bool:
    """Valida CPF com algoritmo de dígitos verificadores."""
    cpf = re.sub(r"\D", "", cpf or "")
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    for i in range(2):
        s = sum(int(cpf[j]) * (10 + i - j) for j in range(9 + i))
        r = (s * 10) % 11
        if r in (10, 11):
            r = 0
        if r != int(cpf[9 + i]):
            return False
    return True


def _cnpj_valido(cnpj: str) -> bool:
    """Valida CNPJ com algoritmo de dígitos verificadores."""
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False
    for i, pesos in enumerate([[5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2],
                                [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]]):
        s = sum(int(cnpj[j]) * pesos[j] for j in range(len(pesos)))
        r = 11 - (s % 11)
        if r >= 10:
            r = 0
        if r != int(cnpj[12 + i]):
            return False
    return True


def _cep_valido(cep: str) -> bool:
    """Valida formato e unicidade dos dígitos do CEP."""
    c = re.sub(r"\D", "", cep or "")
    return len(c) == 8 and c != "00000000" and len(set(c)) > 1


def _idade(nasc: str) -> int:
    """Calcula idade a partir de uma data ISO (YYYY-MM-DD)."""
    try:
        d = date.fromisoformat(nasc)
        h = date.today()
        return h.year - d.year - ((h.month, h.day) < (d.month, d.day))
    except Exception:
        return 99  # Considera maior de idade em caso de erro de parse


# ── Função principal ──────────────────────────────────────────

def auditar(f: dict, docs: list) -> dict:
    """
    Executa as 27 regras de auditoria sobre um pré-cadastro.

    Parâmetros:
        f    — dict com os dados do pré-cadastro (row do hc_precadastros)
        docs — lista de dicts com os documentos (rows do hc_precadastro_docs)

    Retorna dict com:
        resultado_final  — aprovado | aprovado_com_ressalva | pendente | reprovado
        total_regras     — quantidade de regras avaliadas
        reprovadas       — quantidade de regras reprovadas
        pendentes        — quantidade de regras pendentes
        alertas          — quantidade de alertas
        regras           — lista de dicts com o resultado de cada regra
    """
    tipo = (f.get("tipo_pessoa") or "F").upper()
    cpf  = re.sub(r"\D", "", f.get("cnpj_cpf") or "")
    cep  = re.sub(r"\D", "", f.get("cep") or "")
    uf   = _limpo(f.get("uf_sigla") or "").upper()
    obs  = _limpo(f.get("obs") or "").upper()
    res  = []

    def add(cod: str, resultado: str, detalhe: str = ""):
        """Adiciona o resultado de uma regra à lista."""
        res.append({
            "regra":     cod,
            "legenda":   LEGENDA.get(cod, cod),
            "resultado": resultado,
            "detalhe":   detalhe,
        })

    # R01 — Menor de idade
    if tipo == "F" and f.get("data_nascimento"):
        idade = _idade(f["data_nascimento"])
        add("R01",
            "reprovado" if idade < 18 else "ok",
            f"Idade: {idade}" if idade < 18 else "")
    else:
        add("R01", "ok")

    # R02 — Endereço muito longo
    end = _limpo(f.get("endereco"))
    add("R02",
        "reprovado" if len(end) > 60 else "ok",
        f"{len(end)} chars" if len(end) > 60 else "")

    # R03 — Nome/Razão social
    razao = _limpo(f.get("razao"))
    add("R03",
        "reprovado" if not razao or len(razao) > 60 else "ok",
        "Vazio" if not razao else (f"{len(razao)} chars" if len(razao) > 60 else ""))

    # R04 — Documento ausente
    add("R04",
        "reprovado" if not cpf else "ok",
        "Documento não informado" if not cpf else "")

    # R05 / R06 — Validação CPF/CNPJ
    if tipo == "J":
        add("R05",
            "reprovado" if not _cnpj_valido(cpf) else "ok",
            f.get("cnpj_cpf", "") if not _cnpj_valido(cpf) else "")
    if tipo == "F":
        add("R06",
            "reprovado" if not _cpf_valido(cpf) else "ok",
            f.get("cnpj_cpf", "") if not _cpf_valido(cpf) else "")

    # R10 — CEP inválido
    add("R10",
        "reprovado" if not _cep_valido(cep) else "ok",
        f.get("cep", "") if not _cep_valido(cep) else "")

    # R12 — Número de endereço inválido
    num = _limpo(f.get("numero"))
    invalido = (
        num.upper() != "SN" and
        (not num or num.upper() in {"", "0", "00", "000", "SEM NUMERO", "S/N"})
    )
    add("R12",
        "reprovado" if invalido else "ok",
        f"'{num}'" if invalido else "")

    # R13 — Complemento com caracteres proibidos
    comp = _limpo(f.get("complemento") or "")
    add("R13",
        "reprovado" if re.search(r"[@#*!/]", comp) else "ok",
        f"'{comp}'" if re.search(r"[@#*!/]", comp) else "")

    # R14 — Bairro ausente
    add("R14", "reprovado" if not _limpo(f.get("bairro")) else "ok")

    # R15 — Endereço ausente
    add("R15", "reprovado" if not end else "ok")

    # R16 — Tipo de pessoa inválido
    add("R16", "reprovado" if tipo not in ("F", "J") else "ok")

    # R17 — CEP com dígitos todos iguais (suspeito)
    add("R17", "reprovado" if len(cep) == 8 and len(set(cep)) == 1 else "ok")

    # R21 — Cidade ausente
    add("R21",
        "reprovado" if not f.get("ixc_cidade_id") and not _limpo(f.get("cidade_nome")) else "ok")

    # R22 — UF ausente
    add("R22", "reprovado" if not uf else "ok")

    # R23 — Telefone celular inválido (mínimo 10 dígitos)
    cel = re.sub(r"\D", "", f.get("telefone_celular") or "")
    add("R23",
        "reprovado" if len(cel) < 10 else "ok",
        f.get("telefone_celular", "") if len(cel) < 10 else "")

    # R24 — E-mail inválido ou ausente
    email = _limpo(f.get("email") or "")
    if email and ("@" not in email or "." not in email.split("@")[-1]):
        add("R24", "pendente", f"E-mail: '{email}'")
    elif not email:
        add("R24", "pendente", "E-mail não informado")
    else:
        add("R24", "ok")

    # R25 — CPF/CNPJ duplicado no IXC
    if cpf:
        try:
            dup = ixc_select_one(
                "SELECT id, razao FROM ixcprovedor.cliente "
                "WHERE REPLACE(REPLACE(cnpj_cpf,'.',''),'-','')=%s LIMIT 1",
                (cpf,)
            )
            if dup:
                ativo = ixc_select_one(
                    "SELECT id FROM ixcprovedor.cliente_contrato "
                    "WHERE id_cliente=%s AND status='A' LIMIT 1",
                    (dup["id"],)
                )
                add("R25",
                    "reprovado" if ativo else "pendente",
                    f"{'Contrato ativo' if ativo else 'Sem contrato ativo'}: "
                    f"{dup['razao']} (ID {dup['id']})")
            else:
                add("R25", "ok")
        except Exception as e:
            log.warning(f"R25: {e}")
            add("R25", "ok")

    # R26 — Documentos obrigatórios ausentes
    tem_doc    = any(d.get("tipo") in ("rg_frente", "cnh") for d in docs)
    tem_selfie = any(d.get("tipo") == "selfie_doc" for d in docs)
    levar      = "LEVAR CONTRATO" in obs
    if not tem_doc and not tem_selfie:
        add("R26",
            "alerta" if levar else "reprovado",
            "Obs: LEVAR CONTRATO" if levar else "Nenhum doc anexado")
    elif not tem_selfie:
        add("R26", "pendente", "Selfie não anexada")
    else:
        add("R26", "ok")

    # R27 — Cliente com débito em aberto no IXC
    # BUG CORRIGIDO: usava 'p.get' mas a variável do escopo é 'f'
    try:
        from app.services.ixc_db import ixc_conn
        cpf_limpo = (f.get("cnpj_cpf") or "").replace(".", "").replace("-", "").replace("/", "").strip()
        if cpf_limpo:
            with ixc_conn() as _c:
                _cur = _c.cursor()
                _cur.execute("""
                    SELECT COUNT(*) AS total, SUM(fa.valor_aberto) AS valor
                    FROM ixcprovedor.fn_areceber fa
                    JOIN ixcprovedor.cliente c ON c.id = fa.id_cliente
                    WHERE REPLACE(REPLACE(REPLACE(c.cnpj_cpf,'.',''),'-',''),'/','') = %s
                      AND fa.status = 'A'
                      AND fa.data_vencimento < CURDATE()
                """, (cpf_limpo,))
                row = _cur.fetchone()
                qtd = int(row["total"] or 0)
                val = float(row["valor"] or 0)
            if qtd > 0:
                add("R27", "alerta", f"{qtd} fatura(s) em aberto — R$ {val:.2f}")
            else:
                add("R27", "ok")
        else:
            add("R27", "ok")
    except Exception as e:
        log.warning(f"R27: {e}")
        add("R27", "ok")

    # R28 — Conta fiscal (sempre OK por enquanto)
    add("R28", "ok")

    # ── Resultado final ───────────────────────────────────────
    rep  = [r for r in res if r["resultado"] == "reprovado"]
    pend = [r for r in res if r["resultado"] == "pendente"]
    ale  = [r for r in res if r["resultado"] == "alerta"]

    if rep:
        final = "reprovado"
    elif pend:
        final = "pendente"
    elif ale:
        final = "aprovado_com_ressalva"
    else:
        final = "aprovado"

    return {
        "resultado_final": final,
        "total_regras":    len(res),
        "reprovadas":      len(rep),
        "pendentes":       len(pend),
        "alertas":         len(ale),
        "regras":          res,
    }
