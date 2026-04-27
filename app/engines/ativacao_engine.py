"""
Hub Comercial — app/engines/ativacao_engine.py
===============================================
Motor de ativação no IXC Soft.

Executado automaticamente após a assinatura digital do contrato.
Responsável por criar em sequência:
  1. INSERT ixcprovedor.cliente          → cadastro do cliente
  2. INSERT ixcprovedor.cliente_contrato → contrato de internet
  3. INSERT ixcprovedor.su_oss_chamado   → OS de instalação/serviço

Retorna o ixc_cliente_id gerado para uso imediato pelo chamador,
evitando leitura do banco SQLite antes do commit propagar.

Constantes IXC usadas (Cliquedf):
    IXC_ID_CONTA             = 12564
    IXC_ID_FILIAL            = 1
    IXC_ID_CARTEIRA_COBRANCA = 6
    IXC_ID_TIPO_DOCUMENTO    = 501
    IXC_ID_TIPO_CONTRATO     = 20
    IXC_ID_ASSUNTO_INSTALL   = 227

Motivo de inclusão do contrato (id_motivo_inclusao):
    OS 227 (Nova instalação)       → 1
    OS 110 (Mudança de titularidade) → 6
    OS 75/15 (Reativação)          → 8
"""
import sqlite3, os, logging, re, base64, requests
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

import sys; sys.path.insert(0, str(BASE_DIR))
from app.services.ixc_db import ixc_insert, ixc_select_one

DB_PATH = BASE_DIR / "hub_comercial.db"
log     = logging.getLogger(__name__)

# ── Constantes IXC Cliquedf ───────────────────────────────────
IXC_ID_CONTA             = int(os.getenv("IXC_ID_CONTA",             12564))
IXC_ID_TIPO_CLIENTE_PF   = int(os.getenv("IXC_ID_TIPO_CLIENTE_PF",   20))
IXC_ID_TIPO_CLIENTE_PJ   = int(os.getenv("IXC_ID_TIPO_CLIENTE_PJ",   10))
IXC_ID_FILIAL            = int(os.getenv("IXC_ID_FILIAL",            1))
IXC_ID_CARTEIRA          = int(os.getenv("IXC_ID_CARTEIRA_COBRANCA", 6))
IXC_ID_TIPO_DOC          = int(os.getenv("IXC_ID_TIPO_DOCUMENTO",    501))
IXC_ID_TIPO_CONTRATO     = int(os.getenv("IXC_ID_TIPO_CONTRATO",     20))
IXC_ID_ASSUNTO_INSTALL   = int(os.getenv("IXC_ID_ASSUNTO_INSTALACAO",227))

# Produto padrão de ativação (conforme INSERT real)
IXC_ID_PRODUTO_ATIV      = int(os.getenv("IXC_ID_PRODUTO_ATIV",      121))

# Mapa de OS assunto → id_motivo_inclusao
MOTIVO_INCLUSAO = {
    227: 1,   # Nova instalação
    110: 6,   # Mudança de titularidade
    75:  8,   # Reconexão/Reativação
    15:  8,   # Reativação
}


# ── Helpers ───────────────────────────────────────────────────

def get_db():
    """Retorna conexão SQLite com row_factory configurado."""
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _log_etapa(conn, pid: int, etapa: str, sucesso: bool,
               ixc_id: int = None, erro: str = None, payload: dict = None):
    """
    Registra o resultado de cada etapa da ativação em hc_ativacoes_log.
    Sempre faz commit para garantir rastreabilidade mesmo em caso de erro posterior.
    """
    import json
    conn.execute("""
        INSERT INTO hc_ativacoes_log
            (precadastro_id, etapa, sucesso, ixc_id_gerado, erro_msg, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (pid, etapa, 1 if sucesso else 0, ixc_id, erro,
          json.dumps(payload, ensure_ascii=False) if payload else None))
    conn.commit()


def _agora() -> str:
    """Retorna datetime atual formatado (usado em campos ultima_atualizacao)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _hoje_brt() -> str:
    """
    Retorna datetime atual no fuso BRT (UTC-3).
    IMPORTANTE: O IXC armazena datas em BRT — nunca usar UTC puro.
    """
    from datetime import timezone
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")


def _hoje_brt_date() -> str:
    """Retorna apenas a data no fuso BRT (YYYY-MM-DD)."""
    from datetime import timezone
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d")


def _senha_padrao(cpf_cnpj: str) -> str:
    """Senha padrão do cliente = CPF/CNPJ sem máscara."""
    return re.sub(r"\D", "", cpf_cnpj or "")


def _fmt_cpf_cnpj(doc: str, tipo: str) -> str:
    """
    Formata CPF ou CNPJ com máscara.
    OBRIGATÓRIO: o IXC exige o campo cnpj_cpf com máscara.
    """
    d = re.sub(r"\D", "", doc or "")
    if tipo == "F" and len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    if tipo == "J" and len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return doc


def _nome_plano(plano_id) -> str:
    """
    Busca o nome real do plano em vd_contratos.
    IMPORTANTE: não usar o cache local hc_planos — o IXC pode ter nome diferente.
    """
    if not plano_id:
        return ""
    try:
        r = ixc_select_one("SELECT nome FROM ixcprovedor.vd_contratos WHERE id=%s", (plano_id,))
        return r["nome"] if r else ""
    except Exception as e:
        log.warning(f"_nome_plano({plano_id}): {e}")
        return ""


def _get_uf_id(cidade_id) -> int:
    """
    Retorna o ID numérico da UF a partir do ID da cidade no IXC.
    IMPORTANTE: o campo uf em ixcprovedor.cidade é um ID numérico, não a sigla.
    Fallback: 28 (Sergipe).
    """
    if not cidade_id:
        return 28
    try:
        r = ixc_select_one("SELECT uf FROM ixcprovedor.cidade WHERE id=%s", (cidade_id,))
        return int(r["uf"]) if r else 28
    except Exception as e:
        log.warning(f"_get_uf_id({cidade_id}): {e}")
        return 28


# ── Etapa 1: INSERT cliente ───────────────────────────────────

def inserir_cliente(p: dict) -> int:
    """
    Insere o cliente na tabela ixcprovedor.cliente.

    Campos críticos:
    - cnpj_cpf: COM máscara (obrigatório pelo IXC)
    - data_cadastro: BRT (não UTC)
    - uf: ID numérico (não sigla)
    - crm='S', id_candato_tipo=18 (Vendedor Externo)
    - sexo: M/F/O/N/P → IXC aceita M/F, demais mapeados para O
    - rg_orgao_emissor: órgão emissor do RG
    - nacionalidade: padrão 'Brasileiro'

    Retorna o ID gerado pelo IXC.
    """
    tipo        = (p.get("tipo_pessoa") or "F").upper()
    cpf_raw     = re.sub(r"\D", "", p.get("cnpj_cpf") or "")
    cpf_mask    = _fmt_cpf_cnpj(cpf_raw, tipo)
    id_tipo_cli = IXC_ID_TIPO_CLIENTE_PF if tipo == "F" else IXC_ID_TIPO_CLIENTE_PJ
    tipo_assin  = "3" if tipo == "F" else "4"   # 3=PF, 4=PJ (tipo_assinante)
    tipo_scm    = "03" if tipo == "F" else "05"  # código SCM Anatel

    # Mapa de sexo: frontend envia M/F/N(ão-binário)/O(utro)/P(refiro não dizer)
    # IXC aceita M ou F; demais ficam como '' (em branco)
    sexo_raw = (p.get("sexo") or "").upper()
    sexo_ixc = sexo_raw if sexo_raw in ("M", "F") else ""

    sql = """
        INSERT INTO ixcprovedor.cliente (
            razao, cnpj_cpf, tipo_pessoa, fone, telefone_celular, whatsapp,
            email, data_nascimento, cep, endereco, numero, bairro,
            complemento, referencia, cidade, uf,
            id_tipo_cliente, id_conta, filial_id,
            senha, hotsite_acesso,
            ativo, status_internet, bloqueio_automatico, aviso_atraso,
            tipo_assinante, tipo_cliente_scm,
            data_cadastro, ultima_atualizacao,
            id_vendedor, participa_cobranca, cob_envia_email, cob_envia_sms,
            contribuinte_icms,
            crm, crm_data_novo, id_candato_tipo, responsavel,
            Sexo, rg_orgao_emissor, nacionalidade
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
    """
    params = (
        p.get("razao") or "",
        cpf_mask,
        tipo,
        p.get("telefone_celular") or "",    # fone
        p.get("telefone_celular") or "",    # telefone_celular
        p.get("whatsapp") or "",
        p.get("email") or "",
        p.get("data_nascimento") or None,
        p.get("cep") or "",
        p.get("endereco") or "",
        p.get("numero") or "",
        p.get("bairro") or "",
        p.get("complemento") or "",
        p.get("referencia") or "",
        p.get("ixc_cidade_id") or 0,
        _get_uf_id(p.get("ixc_cidade_id")),
        id_tipo_cli,
        IXC_ID_CONTA,
        IXC_ID_FILIAL,
        _senha_padrao(cpf_raw),             # senha = CPF sem máscara
        cpf_raw,                            # hotsite_acesso = CPF sem máscara
        "S", "A", "S", "S",                 # ativo, status_internet, bloqueio_automatico, aviso_atraso
        tipo_assin, tipo_scm,
        _hoje_brt(), _hoje_brt(),           # data_cadastro, ultima_atualizacao (BRT)
        p.get("ixc_vendedor_id") or 0,      # id_vendedor
        "S", "S", "S",                      # participa_cobranca, cob_envia_email, cob_envia_sms
        "n",                                # contribuinte_icms
        "S", _hoje_brt(), 18,               # crm, crm_data_novo, id_candato_tipo=18 (Vendedor Externo)
        p.get("ixc_vendedor_id") or 0,      # responsavel = mesmo vendedor
        sexo_ixc,
        p.get("rg_orgao_emissor") or "",
        p.get("nacionalidade") or "Brasileiro",
    )
    return ixc_insert(sql, params)


# ── Etapa 2: INSERT contrato ──────────────────────────────────

def inserir_contrato(p: dict, ixc_cliente_id: int) -> int:
    """
    Insere o contrato na tabela ixcprovedor.cliente_contrato.

    Campos críticos:
    - id_tipo_documento = 501 (fixo Cliquedf)
    - id_carteira_cobranca = 6 (fixo Cliquedf)
    - id_vendedor = id_vendedor_ativ = vendedor que fez a venda
    - id_motivo_inclusao: 1=NV, 6=TIT, 8=Reativação
    - id_tipo_doc_ativ = 501
    - id_produto_ativ = 121 (conforme insert real)
    - id_cond_pag_ativ = 1 (à vista)
    - ativacao_valor_parcela = taxa_instalacao
    - ativacao_numero_parcelas = 1 se taxa > 0
    - desconto_fidelidade = taxa_instalacao (conforme insert real)
    - fidelidade = 12 meses
    - data_expiracao = data atual + 365 dias
    - id_modelo = 13 (modelo de contrato "ADESÃO E PERMANÊNCIA")

    Retorna o ID gerado pelo IXC.
    """
    taxa    = float(p.get("taxa_instalacao") or 0)
    valor   = float(p.get("plano_valor") or 0)
    fidel   = int(p.get("fidelidade") or 12)
    venc    = int(p.get("dia_vencimento") or 10)
    plano   = p.get("ixc_plano_id") or 0
    vend    = p.get("ixc_vendedor_id") or 0

    # Data de expiração = data atual + 365 dias
    hoje_brt    = _hoje_brt_date()
    data_expira = (date.fromisoformat(hoje_brt) + timedelta(days=365)).strftime("%Y-%m-%d")

    # Vencimento da primeira fatura
    primeiro_venc = f"{hoje_brt.rsplit('-', 1)[0]}-{venc:02d}"

    # Motivo de inclusão conforme tipo de OS
    os_assunto       = int(p.get("os_assunto") or IXC_ID_ASSUNTO_INSTALL)
    id_motivo_incl   = MOTIVO_INCLUSAO.get(os_assunto, 1)

    # Nome real do plano no IXC (não usar cache local)
    nome_plano = _nome_plano(p.get("ixc_plano_id"))

    sql = """
        INSERT INTO ixcprovedor.cliente_contrato (
            id_cliente, id_vd_contrato, status, data,
            renovacao_automatica, valor_unitario,
            id_tipo_contrato, id_filial, id_tipo_documento,
            id_carteira_cobranca, id_vendedor, id_vendedor_ativ,
            status_internet, bloqueio_automatico, aviso_atraso,
            taxa_instalacao, desconto_fidelidade, fidelidade,
            tipo, data_cadastro_sistema, ultima_atualizacao,
            endereco_padrao_cliente, data_expiracao,
            ativacao_numero_parcelas, ativacao_valor_parcela,
            id_tipo_doc_ativ, id_produto_ativ, id_cond_pag_ativ,
            condicao_pagamento_primeira_fat,
            obs, tipo_localidade,
            descricao_aux_plano_venda, id_modelo,
            contrato, id_motivo_inclusao
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
    """
    params = (
        ixc_cliente_id, plano, "P", hoje_brt,
        "S", valor,
        IXC_ID_TIPO_CONTRATO, IXC_ID_FILIAL, IXC_ID_TIPO_DOC,  # 501
        IXC_ID_CARTEIRA, vend, vend,                             # id_carteira=6, id_vendedor=id_vendedor_ativ
        "AA", "S", "S",                  # status_internet=AA (Ag.Assinatura), bloq, aviso
        taxa,                            # taxa_instalacao
        taxa,                            # desconto_fidelidade = taxa (conforme insert real)
        fidel,                           # fidelidade
        "I", _agora(), _agora(),         # tipo=Instalação, datas
        "S", data_expira,                # endereco_padrao_cliente, data_expiracao (+365 dias)
        1 if taxa > 0 else 0,            # ativacao_numero_parcelas
        taxa if taxa > 0 else 0,         # ativacao_valor_parcela = taxa
        IXC_ID_TIPO_DOC,                 # id_tipo_doc_ativ = 501
        IXC_ID_PRODUTO_ATIV,             # id_produto_ativ = 121
        1,                               # id_cond_pag_ativ = 1 (à vista)
        primeiro_venc,                   # condicao_pagamento_primeira_fat
        p.get("obs") or "", "U",         # obs, tipo_localidade=U (urbano)
        nome_plano, 13,                  # descricao_aux_plano_venda, id_modelo=13
        nome_plano,                      # contrato (nome do plano)
        id_motivo_incl,                  # 1=NV, 6=TIT, 8=Reativação
    )
    return ixc_insert(sql, params)


# ── Etapa 3: INSERT OS ────────────────────────────────────────

def inserir_os_instalacao(p: dict, ixc_cliente_id: int, ixc_contrato_id: int) -> int:
    """
    Insere a OS de instalação/serviço em ixcprovedor.su_oss_chamado.

    IMPORTANTE: a tabela su_oss_chamado não tem campo 'numero'.
    O vínculo com o contrato é feito via id_contrato_kit (não id_cliente).

    Retorna o ID da OS gerada.
    """
    vend = p.get("ixc_vendedor_id") or 0
    nome_plano = _nome_plano(p.get("ixc_plano_id"))

    sql = """
        INSERT INTO ixcprovedor.su_oss_chamado (
            id_cliente, id_contrato_kit, id_assunto, status, id_filial,
            data_abertura, mensagem,
            id_tecnico, setor,
            endereco, bairro, referencia, complemento,
            origem_cadastro, ultima_atualizacao
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
    """
    msg = (
        f"OS gerada automaticamente pelo Hub Comercial.\n"
        f"Contrato: {ixc_contrato_id} | Plano: {nome_plano} | Vendedor ID: {vend}"
    )
    params = (
        ixc_cliente_id,
        ixc_contrato_id,                # id_contrato_kit (não id_cliente!)
        IXC_ID_ASSUNTO_INSTALL,         # 227 = Nova Instalação
        "A",                            # status = Aberta
        IXC_ID_FILIAL,
        _agora(),                       # data_abertura
        msg,
        0, "27",                         # id_tecnico (sem técnico inicial), setor=27
        p.get("endereco") or "",
        p.get("bairro") or "",
        p.get("referencia") or "",
        p.get("complemento") or "",
        "W",                            # origem_cadastro = Web
        _agora(),
    )
    return ixc_insert(sql, params)



# ── Etapa 4: Envio de documentos ao IXC ──────────────────────────────────────

def enviar_documentos_ixc(precadastro_id: int, ixc_cliente_id: int) -> list:
    """
    Envia os documentos do pre-cadastro para ixcprovedor.cliente_arquivos via REST.
    Arquivos: rg_frente.jpg, comp_residencia.jpg, selfie_doc.jpg, contrato_{id}.pdf
    """
    ixc_url   = os.getenv("IXC_URL",   "https://sistema.cliquedf.com.br")
    ixc_user  = os.getenv("IXC_USER",  "64")
    ixc_token = os.getenv("IXC_TOKEN", "90b12b22159c00f223eb3e0411f3f1999f68098d1a27127dbec670997ddd800c")

    auth_b64 = base64.b64encode(f"{ixc_user}:{ixc_token}".encode()).decode()
    headers  = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type":  "application/json",
        "ixcsoft":       "gravar",
    }
    endpoint    = f"{ixc_url}/webservice/v1/cliente_arquivos"
    uploads_dir = BASE_DIR / "uploads" / str(precadastro_id)

    DOCS = [
        ("rg_frente.jpg",                 "Documento de Identidade (RG/CNH)", "I"),
        ("comp_residencia.jpg",            "Comprovante de Endereco",          "I"),
        ("selfie_doc.jpg",                 "Selfie com Documento",             "I"),
        (f"contrato_{precadastro_id}.pdf", "Contrato Assinado",                "D"),
    ]

    resultados = []

    for nome_arquivo, descricao, classificacao in DOCS:
        caminho = uploads_dir / nome_arquivo
        if not caminho.exists():
            log.info(f"enviar_documentos_ixc: {nome_arquivo} nao encontrado — pulando")
            resultados.append({"arquivo": nome_arquivo, "sucesso": False, "ixc_id": None, "erro": "arquivo nao encontrado"})
            continue

        try:
            with open(caminho, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()

            payload = {
                "id_cliente":            str(ixc_cliente_id),
                "descricao":             descricao,
                "classificacao_arquivo": classificacao,
                "local_arquivo":         img_b64,
                "nome_arquivo":          nome_arquivo,
                "tipo":                  "imagem" if classificacao == "I" else "documento",
            }

            resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                if data.get("type") == "success":
                    ixc_id = int(data.get("id", 0))
                    log.info(f"enviar_documentos_ixc: {nome_arquivo} OK — ID={ixc_id}")
                    resultados.append({"arquivo": nome_arquivo, "sucesso": True, "ixc_id": ixc_id, "erro": None})
                else:
                    erro = data.get("message", "resposta sem type=success")
                    log.warning(f"enviar_documentos_ixc: {nome_arquivo} erro IXC: {erro}")
                    resultados.append({"arquivo": nome_arquivo, "sucesso": False, "ixc_id": None, "erro": erro})
            else:
                erro = f"HTTP {resp.status_code}"
                log.warning(f"enviar_documentos_ixc: {nome_arquivo} {erro}")
                resultados.append({"arquivo": nome_arquivo, "sucesso": False, "ixc_id": None, "erro": erro})

        except Exception as e:
            log.error(f"enviar_documentos_ixc: {nome_arquivo} excecao: {e}")
            resultados.append({"arquivo": nome_arquivo, "sucesso": False, "ixc_id": None, "erro": str(e)})

    enviados = sum(1 for r in resultados if r["sucesso"])
    log.info(f"enviar_documentos_ixc: #{precadastro_id} — {enviados}/{len(DOCS)} enviados ao IXC")
    return resultados


# ── Função principal ──────────────────────────────────────────

def ativar_cliente(precadastro_id: int) -> int:
    """
    Executa o fluxo completo de ativação de um pré-cadastro no IXC.

    Sequência:
        1. INSERT ixcprovedor.cliente         → salva ixc_cliente_id no SQLite
        2. INSERT ixcprovedor.cliente_contrato → salva ixc_contrato_id no SQLite
        3. INSERT ixcprovedor.su_oss_chamado  → salva ixc_os_id + status='ativado'

    Retorna o ixc_cliente_id gerado (ou 0 em caso de falha).
    O chamador deve usar este retorno diretamente — não reler o banco SQLite —
    para evitar problemas de timing com o commit.

    Em caso de erro em qualquer etapa, o status é atualizado para 'erro_ativacao'
    e o erro é registrado em hc_ativacoes_log.
    """
    conn = get_db()
    ixc_cli_id = 0

    try:
        row = conn.execute(
            "SELECT * FROM hc_precadastros WHERE id=?", (precadastro_id,)
        ).fetchone()

        if not row:
            log.error(f"Pré-cadastro #{precadastro_id} não encontrado.")
            return 0

        p = dict(row)
        log.info(f"Ativando #{precadastro_id} — {p.get('razao')}")

        # Evita ativação dupla
        if p.get("ixc_cliente_id"):
            log.warning(f"#{precadastro_id} já ativado — cliente IXC {p['ixc_cliente_id']}")
            return int(p["ixc_cliente_id"])

        # Bloqueia se cidade não foi resolvida
        if not p.get("ixc_cidade_id"):
            erro = f"ixc_cidade_id ausente — cidade '{p.get('cidade_nome')}' não encontrada no IXC"
            log.error(f"#{precadastro_id} BLOQUEADO: {erro}")
            conn.execute(
                "UPDATE hc_precadastros SET status='erro_ativacao' WHERE id=?",
                (precadastro_id,)
            )
            conn.commit()
            _log_etapa(conn, precadastro_id, "validacao_cidade", False, erro=erro)
            return 0

        # ── Etapa 1: INSERT cliente ───────────────────────────
        try:
            ixc_cli_id = inserir_cliente(p)
            conn.execute(
                "UPDATE hc_precadastros SET ixc_cliente_id=? WHERE id=?",
                (ixc_cli_id, precadastro_id)
            )
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_cliente", True, ixc_id=ixc_cli_id)
            log.info(f"#{precadastro_id} cliente criado no IXC: ID={ixc_cli_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_cliente", False, erro=str(e))
            conn.execute(
                "UPDATE hc_precadastros SET status='erro_ativacao' WHERE id=?",
                (precadastro_id,)
            )
            conn.commit()
            log.error(f"#{precadastro_id} ERRO insert_cliente: {e}")
            return 0

        # ── Etapa 2: INSERT contrato ──────────────────────────
        try:
            ixc_cont_id = inserir_contrato(p, ixc_cli_id)
            conn.execute(
                "UPDATE hc_precadastros SET ixc_contrato_id=? WHERE id=?",
                (ixc_cont_id, precadastro_id)
            )
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_contrato", True, ixc_id=ixc_cont_id)
            log.info(f"#{precadastro_id} contrato criado no IXC: ID={ixc_cont_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_contrato", False, erro=str(e))
            conn.execute(
                "UPDATE hc_precadastros SET status='erro_ativacao' WHERE id=?",
                (precadastro_id,)
            )
            conn.commit()
            log.error(f"#{precadastro_id} ERRO insert_contrato: {e}")
            return ixc_cli_id  # cliente foi criado, retorna o ID mesmo assim

        # ── Etapa 3: INSERT OS instalação ─────────────────────
        ixc_os_id = 0
        try:
            ixc_os_id = inserir_os_instalacao(p, ixc_cli_id, ixc_cont_id)
            conn.execute(
                """UPDATE hc_precadastros
                   SET ixc_os_id=?, status='ativado',
                       atualizado_em=datetime('now','-3 hours')
                   WHERE id=?""",
                (ixc_os_id, precadastro_id)
            )
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_os", True, ixc_id=ixc_os_id)
            log.info(f"#{precadastro_id} OS criada no IXC: ID={ixc_os_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_os", False, erro=str(e))
            log.error(f"#{precadastro_id} ERRO insert_os: {e}")
            # Cliente e contrato já criados — não revertemos, apenas logamos

        # ── Etapa 4: Envio de documentos ─────────────────────
        try:
            docs = enviar_documentos_ixc(precadastro_id, ixc_cli_id)
            enviados = sum(1 for d in docs if d["sucesso"])
            _log_etapa(conn, precadastro_id, "enviar_documentos", True,
                       ixc_id=ixc_cli_id,
                       payload={"enviados": enviados, "total": len(docs), "detalhes": docs})
            log.info(f"#{precadastro_id} documentos enviados: {enviados}/{len(docs)}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "enviar_documentos", False, erro=str(e))
            log.error(f"#{precadastro_id} ERRO enviar_documentos: {e}")

        log.info(
            f"#{precadastro_id} ATIVADO — "
            f"cliente={ixc_cli_id} contrato={ixc_cont_id} OS={ixc_os_id}"
        )
        return ixc_cli_id

    finally:
        conn.close()
