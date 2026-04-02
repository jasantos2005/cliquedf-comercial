"""
Hub Comercial — app/bootstrap/create_admin.py
Cria banco SQLite + grupos + admin inicial.

Uso:
    cd /opt/automacoes/cliquedf/comercial
    venv/bin/python -m app.bootstrap.create_admin
"""
import sqlite3, hashlib, sys
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = BASE_DIR / "hub_comercial.db"

def h(s): return hashlib.sha256(s.encode()).hexdigest()

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS hc_grupos(id INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT NOT NULL UNIQUE,descricao TEXT,nivel INTEGER DEFAULT 1,criado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_usuarios(id INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT NOT NULL,login TEXT NOT NULL UNIQUE,senha_hash TEXT NOT NULL,id_grupo INTEGER REFERENCES hc_grupos(id),ixc_funcionario_id INTEGER,ativo INTEGER DEFAULT 1,criado_em TEXT DEFAULT(datetime('now','-3 hours')),ultimo_acesso TEXT);
CREATE TABLE IF NOT EXISTS hc_planos(id INTEGER PRIMARY KEY,nome TEXT NOT NULL,descricao TEXT,valor REAL,taxa_instalacao REAL DEFAULT 0,fidelidade INTEGER DEFAULT 0,id_tipo_documento INTEGER,id_carteira_cobranca INTEGER,id_vendedor_padrao INTEGER,ativo TEXT DEFAULT 'S',sincronizado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_vendedores(id INTEGER PRIMARY KEY,nome TEXT NOT NULL,login_ixc TEXT,ativo INTEGER DEFAULT 1,sincronizado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_cidades_cache(id INTEGER PRIMARY KEY,nome TEXT NOT NULL,uf TEXT NOT NULL,sincronizado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_precadastros(id INTEGER PRIMARY KEY AUTOINCREMENT,status TEXT DEFAULT 'enviado',id_vendedor_hub INTEGER REFERENCES hc_usuarios(id),ixc_vendedor_id INTEGER,canal_venda TEXT,protocolo TEXT UNIQUE,tipo_pessoa TEXT,razao TEXT,cnpj_cpf TEXT,telefone_celular TEXT,whatsapp TEXT,email TEXT,data_nascimento TEXT,sexo TEXT,cep TEXT,endereco TEXT,numero TEXT,bairro TEXT,complemento TEXT,referencia TEXT,cidade_nome TEXT,uf_sigla TEXT,ixc_cidade_id INTEGER,ixc_uf_id INTEGER,latitude REAL,longitude REAL,viabilidade_status TEXT,viabilidade_nivel INTEGER DEFAULT 0,viabilidade_obs TEXT,viabilidade_checado_em TEXT,ixc_plano_id INTEGER,plano_nome TEXT,plano_valor REAL,taxa_instalacao REAL,fidelidade INTEGER,dia_vencimento INTEGER,obs TEXT,ixc_cliente_id INTEGER,ixc_contrato_id INTEGER,ixc_os_id INTEGER,token_assinatura TEXT UNIQUE,token_expira_em TEXT,assinado_em TEXT,assinatura_ip TEXT,assinatura_arquivo TEXT,criado_em TEXT DEFAULT(datetime('now','-3 hours')),atualizado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE INDEX IF NOT EXISTS idx_precad_status    ON hc_precadastros(status);
CREATE INDEX IF NOT EXISTS idx_precad_vendedor  ON hc_precadastros(id_vendedor_hub);
CREATE INDEX IF NOT EXISTS idx_precad_cpf       ON hc_precadastros(cnpj_cpf);
CREATE TABLE IF NOT EXISTS hc_precadastro_docs(id INTEGER PRIMARY KEY AUTOINCREMENT,precadastro_id INTEGER NOT NULL REFERENCES hc_precadastros(id) ON DELETE CASCADE,tipo TEXT NOT NULL,arquivo TEXT NOT NULL,tamanho_kb INTEGER,criado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_auditoria_log(id INTEGER PRIMARY KEY AUTOINCREMENT,precadastro_id INTEGER NOT NULL REFERENCES hc_precadastros(id),rodada INTEGER DEFAULT 1,regra TEXT NOT NULL,legenda TEXT,resultado TEXT NOT NULL,detalhes TEXT,criado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE INDEX IF NOT EXISTS idx_audit_precad ON hc_auditoria_log(precadastro_id);
CREATE TABLE IF NOT EXISTS hc_ativacoes_log(id INTEGER PRIMARY KEY AUTOINCREMENT,precadastro_id INTEGER NOT NULL REFERENCES hc_precadastros(id),etapa TEXT NOT NULL,payload_json TEXT,ixc_id_gerado INTEGER,sucesso INTEGER DEFAULT 0,erro_msg TEXT,tentativa INTEGER DEFAULT 1,criado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE TABLE IF NOT EXISTS hc_monitoramento(id INTEGER PRIMARY KEY AUTOINCREMENT,ixc_cliente_id INTEGER NOT NULL,ixc_contrato_id INTEGER NOT NULL,ixc_vendedor_id INTEGER,data_snapshot TEXT NOT NULL,status_contrato TEXT,status_internet TEXT,faturas_abertas INTEGER DEFAULT 0,faturas_atrasadas INTEGER DEFAULT 0,valor_em_atraso REAL DEFAULT 0,dias_maior_atraso INTEGER DEFAULT 0,chamados_suporte INTEGER DEFAULT 0,data_cancelamento TEXT,motivo_cancelamento TEXT,criado_em TEXT DEFAULT(datetime('now','-3 hours')));
CREATE INDEX IF NOT EXISTS idx_mon_vendedor ON hc_monitoramento(ixc_vendedor_id);
CREATE TABLE IF NOT EXISTS hc_config(chave TEXT PRIMARY KEY,valor TEXT,descricao TEXT,atualizado_em TEXT DEFAULT(datetime('now','-3 hours')));
"""

def main():
    if DB_PATH.exists():
        r = input(f"Banco já existe. Recriar? [s/N]: ").strip().lower()
        if r != "s": print("Cancelado."); sys.exit(0)
        DB_PATH.unlink(); print("[OK] Banco anterior removido.")

    conn = sqlite3.connect(str(DB_PATH)); conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA); conn.commit(); print("[OK] Tabelas criadas.")

    grupos=[("admin","Acesso total",99),("supervisor","Aprova cadastros",50),
            ("backoffice","Auditoria e ativação",30),("vendedor","App pré-cadastro",10),
            ("desenvolvimento","Acesso total+debug",100)]
    for nome,desc,nivel in grupos:
        conn.execute("INSERT OR IGNORE INTO hc_grupos(nome,descricao,nivel)VALUES(?,?,?)",(nome,desc,nivel))
    conn.commit(); print("[OK] Grupos criados.")

    gid = conn.execute("SELECT id FROM hc_grupos WHERE nome='admin'").fetchone()["id"]
    conn.execute("INSERT OR IGNORE INTO hc_usuarios(nome,login,senha_hash,id_grupo,ativo)VALUES(?,?,?,?,1)",
                 ("Administrador","admin",h("admin123"),gid))
    conn.commit()
    print("[OK] Admin criado — login: admin | senha: admin123")
    print("[!]  Troque a senha após o primeiro acesso!")

    configs=[("versao","1.0.0","Versão"),("operacao","Cliquedf","Operação"),
             ("link_expira_h","48","Horas p/ link assinatura")]
    for k,v,d in configs:
        conn.execute("INSERT OR IGNORE INTO hc_config(chave,valor,descricao)VALUES(?,?,?)",(k,v,d))
    conn.commit(); conn.close()

    print(f"\n[OK] Banco criado: {DB_PATH}")
    print("[>>] Próximo: edite o .env e rode cron_sync_planos_vendedores")

if __name__ == "__main__": main()
