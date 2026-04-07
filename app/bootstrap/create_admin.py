"""
Hub Comercial — app/bootstrap/create_admin.py
============================================
Cria o banco SQLite, grupos de acesso e usuário administrador inicial.

Uso:
    cd /opt/automacoes/cliquedf/comercial
    venv/bin/python -m app.bootstrap.create_admin

ATENÇÃO: Este script destrói o banco existente se confirmado.
         Nunca rodar em produção sem backup.
"""
import sqlite3, hashlib, sys
from pathlib import Path
from dotenv import load_dotenv

# ── Caminhos ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = BASE_DIR / "hub_comercial.db"


def h(s: str) -> str:
    """Gera hash SHA-256 de uma senha."""
    return hashlib.sha256(s.encode()).hexdigest()


# ── Schema completo ───────────────────────────────────────────
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ── Grupos de acesso ─────────────────────────────────────────
-- Níveis: vendedor(10) | backoffice(30) | supervisor(50) | admin(99) | dev(100)
CREATE TABLE IF NOT EXISTS hc_grupos (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    nome        TEXT NOT NULL UNIQUE,
    descricao   TEXT,
    nivel       INTEGER DEFAULT 1,
    criado_em   TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Usuários do sistema ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS hc_usuarios (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nome                TEXT NOT NULL,
    login               TEXT NOT NULL UNIQUE,
    senha_hash          TEXT NOT NULL,              -- SHA-256
    id_grupo            INTEGER REFERENCES hc_grupos(id),
    ixc_funcionario_id  INTEGER,                    -- ID do funcionario no IXC (opcional)
    ativo               INTEGER DEFAULT 1,
    criado_em           TEXT DEFAULT(datetime('now','-3 hours')),
    ultimo_acesso       TEXT
);

-- ── Cache de planos (sincronizado do IXC) ─────────────────────
CREATE TABLE IF NOT EXISTS hc_planos (
    id                  INTEGER PRIMARY KEY,
    nome                TEXT NOT NULL,
    descricao           TEXT,
    valor               REAL,
    taxa_instalacao     REAL DEFAULT 0,
    fidelidade          INTEGER DEFAULT 0,
    id_tipo_documento   INTEGER,
    id_carteira_cobranca INTEGER,
    id_vendedor_padrao  INTEGER,
    ativo               TEXT DEFAULT 'S',
    sincronizado_em     TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Cache de funcionários/vendedores (sincronizado do IXC) ────
CREATE TABLE IF NOT EXISTS hc_vendedores (
    id              INTEGER PRIMARY KEY,
    nome            TEXT NOT NULL,
    login_ixc       TEXT,
    ativo           INTEGER DEFAULT 1,
    sincronizado_em TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Vendedores autorizados a aparecer nos filtros do painel ───
-- Gerenciado via Admin → Vendedores ativos
CREATE TABLE IF NOT EXISTS hc_vendedores_ativos (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ixc_id    INTEGER UNIQUE NOT NULL,      -- ID do vendedor na tabela vendedor do IXC
    nome      TEXT NOT NULL,
    ativo     INTEGER DEFAULT 1,
    criado_em TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Cache de cidades (sincronizado do IXC) ────────────────────
CREATE TABLE IF NOT EXISTS hc_cidades_cache (
    id              INTEGER PRIMARY KEY,
    nome            TEXT NOT NULL,
    uf              TEXT NOT NULL,
    sincronizado_em TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Pré-cadastros (tabela principal do fluxo comercial) ───────
-- Fluxo de status:
--   enviado → em_auditoria → aprovado/pendente/reprovado
--   → assinatura_pendente → assinado → ativado / erro_ativacao
CREATE TABLE IF NOT EXISTS hc_precadastros (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    status              TEXT DEFAULT 'enviado',
    id_vendedor_hub     INTEGER REFERENCES hc_usuarios(id),
    ixc_vendedor_id     INTEGER,            -- ID do vendedor na tabela vendedor do IXC
    canal_venda         TEXT,
    protocolo           TEXT UNIQUE,

    -- Dados pessoais do cliente
    tipo_pessoa         TEXT,               -- F=Física, J=Jurídica
    razao               TEXT,               -- Nome completo ou razão social
    cnpj_cpf            TEXT,
    telefone_celular    TEXT,
    whatsapp            TEXT,
    email               TEXT,
    data_nascimento     TEXT,
    sexo                TEXT,               -- M=Masculino, F=Feminino, O=Outro, N=Não-binário, P=Prefiro não dizer
    rg_orgao_emissor    TEXT,               -- Ex: SSP/SE — campo rg_orgao_emissor no IXC
    nacionalidade       TEXT DEFAULT 'Brasileiro',

    -- Endereço
    cep                 TEXT,
    endereco            TEXT,
    numero              TEXT,
    bairro              TEXT,
    complemento         TEXT,
    referencia          TEXT,
    cidade_nome         TEXT,
    uf_sigla            TEXT,
    ixc_cidade_id       INTEGER,            -- ID da cidade no IXC
    ixc_uf_id           INTEGER,            -- ID da UF no IXC (numérico)
    latitude            REAL,
    longitude           REAL,

    -- Viabilidade de cobertura
    viabilidade_status  TEXT,               -- ok | alerta | bloqueado
    viabilidade_nivel   INTEGER DEFAULT 0,
    viabilidade_obs     TEXT,
    viabilidade_checado_em TEXT,

    -- Plano contratado
    ixc_plano_id        INTEGER,
    plano_nome          TEXT,
    plano_valor         REAL,
    taxa_instalacao     REAL,
    fidelidade          INTEGER,
    dia_vencimento      INTEGER,
    obs                 TEXT,

    -- IDs gerados no IXC após ativação
    ixc_cliente_id      INTEGER,            -- ID na tabela cliente do IXC
    ixc_contrato_id     INTEGER,            -- ID na tabela cliente_contrato do IXC
    ixc_os_id           INTEGER,            -- ID na tabela su_oss_chamado do IXC

    -- Assinatura digital
    token_assinatura    TEXT UNIQUE,
    token_expira_em     TEXT,
    assinado_em         TEXT,
    assinatura_ip       TEXT,
    assinatura_arquivo  TEXT,               -- Caminho do PNG da assinatura

    criado_em           TEXT DEFAULT(datetime('now','-3 hours')),
    atualizado_em       TEXT DEFAULT(datetime('now','-3 hours'))
);

CREATE INDEX IF NOT EXISTS idx_precad_status    ON hc_precadastros(status);
CREATE INDEX IF NOT EXISTS idx_precad_vendedor  ON hc_precadastros(id_vendedor_hub);
CREATE INDEX IF NOT EXISTS idx_precad_cpf       ON hc_precadastros(cnpj_cpf);

-- ── Documentos do pré-cadastro ────────────────────────────────
-- Tipos: rg_frente | selfie_doc | comp_residencia
CREATE TABLE IF NOT EXISTS hc_precadastro_docs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    precadastro_id  INTEGER NOT NULL REFERENCES hc_precadastros(id) ON DELETE CASCADE,
    tipo            TEXT NOT NULL,
    arquivo         TEXT NOT NULL,          -- Caminho relativo: uploads/{pid}/{tipo}.jpg
    tamanho_kb      INTEGER,
    criado_em       TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Log de auditoria (27 regras) ──────────────────────────────
-- resultado: ok | reprovado | pendente | alerta
CREATE TABLE IF NOT EXISTS hc_auditoria_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    precadastro_id  INTEGER NOT NULL REFERENCES hc_precadastros(id),
    rodada          INTEGER DEFAULT 1,      -- Incrementa a cada reauditoria
    regra           TEXT NOT NULL,          -- Ex: R01, R25, SERASA_OK
    legenda         TEXT,
    resultado       TEXT NOT NULL,
    detalhes        TEXT,
    criado_em       TEXT DEFAULT(datetime('now','-3 hours'))
);

CREATE INDEX IF NOT EXISTS idx_audit_precad ON hc_auditoria_log(precadastro_id);

-- ── Log de ativação IXC ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS hc_ativacoes_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    precadastro_id  INTEGER NOT NULL REFERENCES hc_precadastros(id),
    etapa           TEXT NOT NULL,          -- insert_cliente | insert_contrato | insert_os
    payload_json    TEXT,
    ixc_id_gerado   INTEGER,
    sucesso         INTEGER DEFAULT 0,
    erro_msg        TEXT,
    tentativa       INTEGER DEFAULT 1,
    criado_em       TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Monitoramento pós-ativação ────────────────────────────────
CREATE TABLE IF NOT EXISTS hc_monitoramento (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ixc_cliente_id      INTEGER NOT NULL,
    ixc_contrato_id     INTEGER NOT NULL,
    ixc_vendedor_id     INTEGER,
    data_snapshot       TEXT NOT NULL,
    status_contrato     TEXT,
    status_internet     TEXT,
    faturas_abertas     INTEGER DEFAULT 0,
    faturas_atrasadas   INTEGER DEFAULT 0,
    valor_em_atraso     REAL DEFAULT 0,
    dias_maior_atraso   INTEGER DEFAULT 0,
    chamados_suporte    INTEGER DEFAULT 0,
    data_cancelamento   TEXT,
    motivo_cancelamento TEXT,
    criado_em           TEXT DEFAULT(datetime('now','-3 hours'))
);

CREATE INDEX IF NOT EXISTS idx_mon_vendedor ON hc_monitoramento(ixc_vendedor_id);

-- ── Cache de contratos IXC (sincronizado a cada 10min) ────────
CREATE TABLE IF NOT EXISTS hc_contratos_cache (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ixc_contrato_id     INTEGER UNIQUE,
    ixc_cliente_id      INTEGER,
    razao               TEXT,
    cnpj_cpf            TEXT,
    cidade_nome         TEXT,
    bairro              TEXT,
    vendedor_id         INTEGER,
    vendedor_nome       TEXT,
    plano_id            INTEGER,
    plano_nome          TEXT,
    plano_valor         REAL,
    status_contrato     TEXT,
    status_acesso       TEXT,
    data_contrato       TEXT,
    data_ativacao       TEXT,
    os_status           TEXT,
    os_tecnico          TEXT,
    os_data_fechamento  TEXT,
    sincronizado_em     TEXT
);

-- ── Log de execução das automações/crons ──────────────────────
CREATE TABLE IF NOT EXISTS hc_automacoes_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    motor       TEXT NOT NULL,              -- Ex: Auditoria | Sync Contratos IXC
    status      TEXT NOT NULL,              -- ok | erro
    linhas      INTEGER DEFAULT 0,
    resumo      TEXT,
    log_texto   TEXT,
    duracao_s   REAL DEFAULT 0,
    criado_em   TEXT DEFAULT(datetime('now','-3 hours'))
);

-- ── Configurações gerais do sistema ──────────────────────────
CREATE TABLE IF NOT EXISTS hc_config (
    chave       TEXT PRIMARY KEY,
    valor       TEXT,
    descricao   TEXT,
    atualizado_em TEXT DEFAULT(datetime('now','-3 hours'))
);
"""


def main():
    if DB_PATH.exists():
        r = input(f"Banco já existe em {DB_PATH}.\nRecriar do zero? TODOS OS DADOS SERÃO PERDIDOS. [s/N]: ").strip().lower()
        if r != "s":
            print("Operação cancelada.")
            sys.exit(0)
        DB_PATH.unlink()
        print("[OK] Banco anterior removido.")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    print("[OK] Tabelas criadas.")

    # ── Grupos de acesso ──────────────────────────────────────
    grupos = [
        ("admin",          "Acesso total ao sistema",           99),
        ("supervisor",     "Aprova e libera cadastros",         50),
        ("backoffice",     "Auditoria e ativação",              30),
        ("vendedor",       "App pré-cadastro mobile",           10),
        ("desenvolvimento","Acesso total + debug",             100),
    ]
    for nome, desc, nivel in grupos:
        conn.execute(
            "INSERT OR IGNORE INTO hc_grupos(nome, descricao, nivel) VALUES(?,?,?)",
            (nome, desc, nivel)
        )
    conn.commit()
    print("[OK] Grupos criados.")

    # ── Usuário administrador inicial ─────────────────────────
    gid = conn.execute("SELECT id FROM hc_grupos WHERE nome='admin'").fetchone()["id"]
    conn.execute(
        "INSERT OR IGNORE INTO hc_usuarios(nome, login, senha_hash, id_grupo, ativo) VALUES(?,?,?,?,1)",
        ("Administrador", "admin", h("admin123"), gid)
    )
    conn.commit()
    print("[OK] Admin criado — login: admin | senha: admin123")
    print("[!]  TROQUE A SENHA após o primeiro acesso!")

    # ── Configurações padrão ──────────────────────────────────
    configs = [
        ("versao",         "2.5.0",  "Versão do sistema"),
        ("operacao",       "Cliquedf", "Nome da operação"),
        ("link_expira_h",  "48",     "Horas de validade do link de assinatura"),
    ]
    for chave, valor, desc in configs:
        conn.execute(
            "INSERT OR IGNORE INTO hc_config(chave, valor, descricao) VALUES(?,?,?)",
            (chave, valor, desc)
        )
    conn.commit()
    conn.close()

    print(f"\n[OK] Banco criado: {DB_PATH}")
    print("[>>] Próximo passo: edite o .env e rode:")
    print("     venv/bin/python -m app.bootstrap.cron_sync_planos_vendedores")


if __name__ == "__main__":
    main()
