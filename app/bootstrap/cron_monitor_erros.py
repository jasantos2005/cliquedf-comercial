"""
Hub Comercial — cron_monitor_erros.py
======================================
Roda a cada 1 hora via crontab.
Detecta erros do sistema, corrige o que for possivel
e notifica Ailton via Telegram pessoal.
"""
import sqlite3, logging, os, sys, requests, unicodedata
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")
import pymysql, pymysql.cursors

DB_PATH = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_AILTON = "2135602169"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def ixc():
    return pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        database="ixcprovedor", cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )


def notificar(msg):
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN nao configurado")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_AILTON, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        log.info("Telegram enviado")
    except Exception as e:
        log.error(f"Telegram erro: {e}")


def verificar_servico():
    import subprocess
    r = subprocess.run(["systemctl", "is-active", "hubcomercial_cliquedf"],
                       capture_output=True, text=True)
    if r.stdout.strip() != "active":
        log.warning("Servico INATIVO — reiniciando...")
        subprocess.run(["systemctl", "restart", "hubcomercial_cliquedf"])
        return "Servico estava inativo — *reiniciado automaticamente* ✅"
    return None


def corrigir_cidade():
    conn = db()
    erros = conn.execute("""
        SELECT DISTINCT p.id, p.razao, p.cidade_nome, p.ixc_cidade_id
        FROM hc_precadastros p
        JOIN hc_ativacoes_log al ON al.precadastro_id = p.id
        WHERE p.status = 'erro_ativacao'
        AND al.sucesso = 0
        AND (al.erro_msg LIKE '%cidade%' OR al.erro_msg LIKE '%cliente_ibfk_3%')
        ORDER BY p.id DESC LIMIT 50
    """).fetchall()

    if not erros:
        conn.close()
        return []

    cx = ixc()
    cur = cx.cursor()
    corrigidos = []

    for e in erros:
        nome = e["cidade_nome"] or ""
        if not nome:
            continue
        cur.execute("SELECT id FROM cidade WHERE nome LIKE %s AND uf=28 LIMIT 1", (f"%{nome}%",))
        cidade = cur.fetchone()
        if not cidade:
            sem_acento = ''.join(c for c in unicodedata.normalize('NFD', nome)
                                 if unicodedata.category(c) != 'Mn')
            cur.execute("SELECT id FROM cidade WHERE nome LIKE %s AND uf=28 LIMIT 1", (f"%{sem_acento}%",))
            cidade = cur.fetchone()
        if cidade:
            conn.execute("""
                UPDATE hc_precadastros
                SET ixc_cidade_id=?, status='aprovado', atualizado_em=datetime('now','-3 hours')
                WHERE id=?
            """, (cidade["id"], e["id"]))
            corrigidos.append(f"#{e['id']} {e['razao']} ({nome})")
            log.info(f"Cidade corrigida: {e['razao']} → id={cidade['id']}")
        else:
            log.warning(f"Cidade nao encontrada no IXC: {nome}")

    conn.commit()
    conn.close()
    cx.close()
    return corrigidos


def corrigir_sem_vendedor():
    """Apenas alerta — nao corrige automaticamente para evitar vendedor errado."""
    conn = db()
    rows = conn.execute("""
        SELECT p.id, p.razao, u.nome as usuario
        FROM hc_precadastros p
        LEFT JOIN hc_usuarios u ON u.id = p.id_vendedor_hub
        WHERE p.ixc_vendedor_id IS NULL
        AND p.criado_em >= datetime('now', '-24 hours', '-3 hours')
        AND p.status NOT IN ('cancelado','excluido','reprovado')
    """).fetchall()
    conn.close()
    alertas = []
    for p in rows:
        alertas.append(f"#{p['id']} {p['razao']} (usuario: {p['usuario'] or '?'})")
    return alertas

def erros_manuais():
    conn = db()
    rows = conn.execute("""
        SELECT p.id, p.razao, al.erro_msg
        FROM hc_precadastros p
        JOIN hc_ativacoes_log al ON al.precadastro_id = p.id
        WHERE p.status = 'erro_ativacao' AND al.sucesso = 0
        AND al.erro_msg NOT LIKE '%cidade%'
        AND al.erro_msg NOT LIKE '%cliente_ibfk_3%'
        AND p.criado_em >= datetime('now', '-24 hours', '-3 hours')
        GROUP BY p.id ORDER BY p.id DESC LIMIT 10
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _salvar_log(status, resumo, duracao=0):
    try:
        import sqlite3
        c = sqlite3.connect(str(DB_PATH))
        c.execute("INSERT INTO hc_automacoes_log(motor,status,resumo,duracao_s) VALUES(?,?,?,?)",
                  ("Monitor de Erros", status, resumo, round(duracao, 2)))
        c.commit(); c.close()
    except Exception as e:
        log.warning(f"_salvar_log: {e}")


def verificar_regras():
    """Verifica integridade das regras criticas do sistema."""
    problemas = []
    try:
        import sqlite3 as _sq
        _conn = _sq.connect(str(DB_PATH))
        _conn.row_factory = _sq.Row

        # Verificar vendedor errado — ixc_vendedor_id nulo em cadastros recentes
        sem_vendedor = _conn.execute("""
            SELECT COUNT(*) as total FROM hc_precadastros
            WHERE ixc_vendedor_id IS NULL
            AND criado_em >= datetime('now', '-24 hours', '-3 hours')
            AND status NOT IN ('cancelado','excluido')
        """).fetchone()
        if sem_vendedor and sem_vendedor['total'] > 0:
            problemas.append(f"Cadastros sem vendedor nas ultimas 24h: {sem_vendedor['total']}")

        # Verificar cadastros duplicados — mesmo CPF em menos de 5 minutos (duplicata real)
        duplicados = _conn.execute("""
            SELECT a.cnpj_cpf, a.razao, a.criado_em, b.criado_em as criado_em2
            FROM hc_precadastros a
            JOIN hc_precadastros b ON b.cnpj_cpf = a.cnpj_cpf AND b.id > a.id
            WHERE a.criado_em >= datetime('now', '-24 hours', '-3 hours')
            AND (strftime('%s', b.criado_em) - strftime('%s', a.criado_em)) <= 300
        """).fetchall()
        if duplicados:
            for d in duplicados:
                problemas.append(f"Cadastro duplicado em menos de 5min: {d['cnpj_cpf']} ({d['razao']})")

        _conn.close()
        base = Path(__file__).resolve().parent.parent.parent

        # R25 — CPF duplicado com financeiro
        with open(base / 'app/engines/auditoria_engine.py') as f:
            audit = f.read()
        if 'tem_divida' not in audit or 'divida em aberto' not in audit:
            problemas.append("R25 auditoria_engine — regra CPF duplicado com financeiro quebrada")
        if 'pode abrir novo contrato' not in audit:
            problemas.append("R25 auditoria_engine — regra CPF duplicado OK quebrada")

        # Engine — reutiliza cliente existente
        with open(base / 'app/engines/ativacao_engine.py') as f:
            engine = f.read()
        if 'cliente ja existe no IXC' not in engine or 'pulando INSERT' not in engine:
            problemas.append("ativacao_engine — reutilizacao de cliente existente quebrada")
        if '_get_usuario_ixc_id' not in engine:
            problemas.append("ativacao_engine — responsavel funcionario_ixc_id quebrado")

        # App — preview auditoria
        with open(base / 'static/app.html') as f:
            app = f.read()
        if 'preview-auditoria' not in app:
            problemas.append("app.html — preview de auditoria antes de enviar quebrado")

        # Sync — preserva mapeamentos
        with open(base / 'app/bootstrap/cron_sync_planos_vendedores.py') as f:
            sync = f.read()
        if 'funcionario_ixc_id' not in sync or 'usuario_ixc_id' not in sync:
            problemas.append("cron_sync — preservacao usuario/funcionario_ixc_id quebrada")
        if 'NOT IN (2, 21, 40, 29, 1)' not in sync:
            problemas.append("cron_sync — exclusao de vendedores genericos quebrada")

    except Exception as e:
        problemas.append(f"Erro ao verificar regras: {e}")

    return problemas


def main():
    log.info("=== Monitor Hub Comercial ===")
    linhas = []
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")

    # 1. Servico
    msg_serv = verificar_servico()
    if msg_serv:
        linhas.append(f"🚨 {msg_serv}")

    # 2. Corrigir cidades
    cidades = corrigir_cidade()
    if cidades:
        linhas.append(f"*Cidade corrigida automaticamente ({len(cidades)}):*")
        for c in cidades:
            linhas.append(f"  ✅ {c}")

    # 3. Corrigir sem vendedor
    vendedores = corrigir_sem_vendedor()
    if vendedores:
        linhas.append(f"*⚠️ Cadastros sem vendedor ({len(vendedores)}):*")
        for v in vendedores:
            linhas.append(f"  ✅ {v}")

    # 4. Erros manuais
    manuais = erros_manuais()
    if manuais:
        linhas.append(f"*⚠️ Erros que precisam atenção manual ({len(manuais)}):*")
        for e in manuais[:5]:
            msg = (e["erro_msg"] or "")[:80]
            linhas.append(f"  ❌ #{e['id']} {e['razao']}: {msg}")

    # Verificar integridade das regras
    regras_quebradas = verificar_regras()
    if regras_quebradas:
        linhas.append(f"*🔴 Regras do sistema quebradas ({len(regras_quebradas)}):*")
        for r in regras_quebradas:
            linhas.append(f"  ⚠️ {r}")

    if not linhas:
        log.info("Nenhum erro encontrado.")
        _salvar_log("ok", "Nenhum erro encontrado.")
        return

    msg = f"\U0001f916 *Hub Comercial Monitor* `{agora}`\n\n" + "\n".join(linhas)
    notificar(msg)
    resumo = f"Correcoes: cidades={len(cidades)} vendedores={len(vendedores)} alertas={len(manuais)}"
    _salvar_log("ok" if not manuais else "alerta", resumo)
    log.info(f"Notificacao enviada. {len(linhas)} itens.")


if __name__ == "__main__":
    main()
