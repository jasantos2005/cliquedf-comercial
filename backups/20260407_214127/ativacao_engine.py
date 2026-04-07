"""
Hub Comercial — app/engines/ativacao_engine.py
Motor de ativação no IXC.
Executa após assinatura digital:
  1. INSERT cliente
  2. INSERT cliente_contrato
  3. INSERT su_oss_chamado (OS instalação)
"""
import sqlite3, os, logging, re
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

import sys; sys.path.insert(0, str(BASE_DIR))
from app.services.ixc_db import ixc_insert, ixc_select_one

DB_PATH = BASE_DIR / "hub_comercial.db"
log     = logging.getLogger(__name__)

# Constantes Cliquedf
IXC_ID_CONTA             = int(os.getenv("IXC_ID_CONTA", 12564))
IXC_ID_TIPO_CLIENTE_PF   = int(os.getenv("IXC_ID_TIPO_CLIENTE_PF", 20))
IXC_ID_TIPO_CLIENTE_PJ   = int(os.getenv("IXC_ID_TIPO_CLIENTE_PJ", 10))
IXC_ID_FILIAL            = int(os.getenv("IXC_ID_FILIAL", 1))
IXC_ID_CARTEIRA          = int(os.getenv("IXC_ID_CARTEIRA_COBRANCA", 6))
IXC_ID_TIPO_DOC          = int(os.getenv("IXC_ID_TIPO_DOCUMENTO", 501))
IXC_ID_TIPO_CONTRATO     = int(os.getenv("IXC_ID_TIPO_CONTRATO", 20))
IXC_ID_ASSUNTO_INSTALL   = int(os.getenv("IXC_ID_ASSUNTO_INSTALACAO", 227))


def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _log_etapa(conn, pid, etapa, sucesso, ixc_id=None, erro=None, payload=None):
    import json
    conn.execute("""
        INSERT INTO hc_ativacoes_log
            (precadastro_id, etapa, sucesso, ixc_id_gerado, erro_msg, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (pid, etapa, 1 if sucesso else 0, ixc_id, erro,
          json.dumps(payload, ensure_ascii=False) if payload else None))
    conn.commit()


def _agora():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _hoje():
    return date.today().strftime("%Y-%m-%d")


def _senha_padrao(cpf_cnpj: str) -> str:
    """Senha padrão = CPF/CNPJ sem máscara."""
    return re.sub(r"\D", "", cpf_cnpj or "")




def _hoje_brt() -> str:
    """Retorna datetime atual no fuso BRT (UTC-3)."""
    from datetime import timezone, timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def _fmt_cpf_cnpj(doc: str, tipo: str) -> str:
    """Formata CPF ou CNPJ com máscara."""
    d = re.sub(r"\D", "", doc or "")
    if tipo == "F" and len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    if tipo == "J" and len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return doc


def _nome_plano(plano_id) -> str:
    """Busca o nome real do plano em vd_contratos."""
    if not plano_id:
        return ""
    try:
        r = ixc_select_one("SELECT nome FROM ixcprovedor.vd_contratos WHERE id=%s", (plano_id,))
        return r["nome"] if r else ""
    except:
        return ""

def _get_uf_id(cidade_id) -> int:
    """Retorna o ID numérico da UF a partir do ID da cidade no IXC."""
    if not cidade_id:
        return 28  # SE como fallback
    try:
        r = ixc_select_one("SELECT uf FROM ixcprovedor.cidade WHERE id=%s", (cidade_id,))
        return int(r["uf"]) if r else 28
    except:
        return 28

def inserir_cliente(p: dict) -> int:
    """INSERT na tabela cliente do IXC. Retorna o ID gerado."""
    tipo       = (p.get("tipo_pessoa") or "F").upper()
    cpf        = re.sub(r"\D", "", p.get("cnpj_cpf") or "")
    cpf_mask   = _fmt_cpf_cnpj(cpf, tipo)
    id_tipo_cli = IXC_ID_TIPO_CLIENTE_PF if tipo == "F" else IXC_ID_TIPO_CLIENTE_PJ
    tipo_assin  = "3" if tipo == "F" else "4"
    tipo_scm    = "03" if tipo == "F" else "05"

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
            crm, crm_data_novo, id_candato_tipo, responsavel
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
            %s, %s, %s, %s
        )
    """
    params = (
        p.get("razao") or "",
        cpf_mask,
        tipo,
        p.get("telefone_celular") or "",
        p.get("telefone_celular") or "",
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
        _get_uf_id(p.get("ixc_cidade_id")) or 28,
        id_tipo_cli,
        IXC_ID_CONTA,
        IXC_ID_FILIAL,
        _senha_padrao(p.get("cnpj_cpf")),
        cpf,
        "S", "A", "S", "S",
        tipo_assin, tipo_scm,
        _hoje_brt(), _hoje_brt(),
        p.get("ixc_vendedor_id") or 0,
        "S", "S", "S",
        "n",
        "S", _hoje_brt(), 18, p.get("ixc_vendedor_id") or 0,
    )
    return ixc_insert(sql, params)


def inserir_contrato(p: dict, ixc_cliente_id: int) -> int:
    """INSERT na tabela cliente_contrato do IXC. Retorna o ID gerado."""
    from decimal import Decimal
    taxa   = float(p.get("taxa_instalacao") or 0)
    valor  = float(p.get("plano_valor") or 0)
    fidel  = int(p.get("fidelidade") or 0)
    venc   = int(p.get("dia_vencimento") or 10)
    plano  = p.get("ixc_plano_id") or 0
    vend   = p.get("ixc_vendedor_id") or 0

    # Data de expiração = hoje + fidelidade meses
    if fidel > 0:
        from dateutil.relativedelta import relativedelta
        expira = (date.today() + relativedelta(months=fidel)).strftime("%Y-%m-%d")
    else:
        expira = None

    # Vencimento da primeira fatura
    primeiro_venc = f"{_hoje().rsplit('-',1)[0]}-{venc:02d}"

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
            contrato, motivo_inclusao
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
    # Motivo inclusao enum: I=Instalacao, T=Titularidade, R=Reativacao
    os_assunto = int(p.get("os_assunto") or 227)
    motivo_map = {227: 'I', 110: 'T', 75: 'R', 15: 'R'}
    motivo_inclusao = motivo_map.get(os_assunto, 'I')

    params = (
        ixc_cliente_id, plano, "P", _hoje(),
        "S", valor,
        IXC_ID_TIPO_CONTRATO, IXC_ID_FILIAL, IXC_ID_TIPO_DOC,
        IXC_ID_CARTEIRA, vend, vend,
        "AA", "S", "S",
        taxa, taxa, fidel,
        "I", _agora(), _agora(),
        "S", expira,
        1 if taxa > 0 else 0,
        taxa if taxa > 0 else 0,
        IXC_ID_TIPO_DOC, plano, 1,
        primeiro_venc,
        p.get("obs") or "", "U",
        _nome_plano(p.get("ixc_plano_id")), 13,
        _nome_plano(p.get("ixc_plano_id")), motivo_inclusao,
    )
    return ixc_insert(sql, params)


def inserir_os_instalacao(p: dict, ixc_cliente_id: int, ixc_contrato_id: int) -> int:
    """INSERT OS de instalação na tabela su_oss_chamado."""
    vend = p.get("ixc_vendedor_id") or 0
    sql = """
        INSERT INTO ixcprovedor.su_oss_chamado (
            id_cliente, id_assunto, status, id_filial,
            data_abertura, mensagem,
            id_tecnico, setor,
            endereco, bairro, referencia, complemento,
            origem_cadastro, ultima_atualizacao
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
    """
    msg = (f"OS de instalação gerada automaticamente pelo Hub Comercial.\n"
           f"Contrato: {ixc_contrato_id} | Plano: {p.get('plano_nome','')} | "
           f"Vendedor ID: {vend}")
    params = (
        ixc_cliente_id, IXC_ID_ASSUNTO_INSTALL, "A", IXC_ID_FILIAL,
        _agora(), msg,
        0, "Instalação",
        p.get("endereco") or "", p.get("bairro") or "",
        p.get("referencia") or "", p.get("complemento") or "",
        "W", _agora(),
    )
    return ixc_insert(sql, params)


def ativar_cliente(precadastro_id: int):
    """
    Função principal — chamada após assinatura digital.
    Executa INSERT cliente → contrato → OS em sequência.
    """
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM hc_precadastros WHERE id=?", (precadastro_id,)).fetchone()
        if not row:
            log.error(f"Pré-cadastro #{precadastro_id} não encontrado.")
            return
        p = dict(row)

        log.info(f"Ativando #{precadastro_id} — {p.get('razao')}")

        # Verifica se já foi ativado
        if p.get("ixc_cliente_id"):
            log.warning(f"#{precadastro_id} já ativado — cliente IXC {p['ixc_cliente_id']}")
            return

        # ── ETAPA 1: INSERT cliente ──────────────────────────
        try:
            ixc_cli_id = inserir_cliente(p)
            conn.execute("UPDATE hc_precadastros SET ixc_cliente_id=? WHERE id=?", (ixc_cli_id, precadastro_id))
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_cliente", True, ixc_id=ixc_cli_id)
            log.info(f"#{precadastro_id} cliente criado no IXC: ID={ixc_cli_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_cliente", False, erro=str(e))
            conn.execute("UPDATE hc_precadastros SET status='erro_ativacao' WHERE id=?", (precadastro_id,))
            conn.commit()
            log.error(f"#{precadastro_id} ERRO insert_cliente: {e}")
            return

        # ── ETAPA 2: INSERT contrato ─────────────────────────
        try:
            ixc_cont_id = inserir_contrato(p, ixc_cli_id)
            conn.execute("UPDATE hc_precadastros SET ixc_contrato_id=? WHERE id=?", (ixc_cont_id, precadastro_id))
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_contrato", True, ixc_id=ixc_cont_id)
            log.info(f"#{precadastro_id} contrato criado no IXC: ID={ixc_cont_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_contrato", False, erro=str(e))
            conn.execute("UPDATE hc_precadastros SET status='erro_ativacao' WHERE id=?", (precadastro_id,))
            conn.commit()
            log.error(f"#{precadastro_id} ERRO insert_contrato: {e}")
            return

        # ── ETAPA 3: INSERT OS instalação ────────────────────
        try:
            ixc_os_id = inserir_os_instalacao(p, ixc_cli_id, ixc_cont_id)
            conn.execute("UPDATE hc_precadastros SET ixc_os_id=?, status='ativado', atualizado_em=datetime('now','-3 hours') WHERE id=?",
                         (ixc_os_id, precadastro_id))
            conn.commit()
            _log_etapa(conn, precadastro_id, "insert_os", True, ixc_id=ixc_os_id)
            log.info(f"#{precadastro_id} OS criada no IXC: ID={ixc_os_id}")
        except Exception as e:
            _log_etapa(conn, precadastro_id, "insert_os", False, erro=str(e))
            log.error(f"#{precadastro_id} ERRO insert_os: {e}")
            # Não muda status — cliente e contrato já criados

        log.info(f"#{precadastro_id} ATIVADO — cliente={ixc_cli_id} contrato={ixc_cont_id} OS={ixc_os_id if 'ixc_os_id' in dir() else 'ERRO'}")

    finally:
        conn.close()
