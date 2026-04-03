"""
Hub Comercial — cron_sync_contratos.py
Sincroniza contratos do IXC para hc_contratos_cache.
Roda a cada 10min via crontab.
"""
import sqlite3, logging, sys, time, io
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))

DB_PATH = BASE_DIR / "hub_comercial.db"

# Captura log em memória para salvar na tabela
log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.StreamHandler(log_stream)]
)
log = logging.getLogger(__name__)


def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def salvar_log(motor, status, resumo, duracao):
    try:
        conn = get_db()
        log_texto = log_stream.getvalue()[-8000:]  # últimas 8000 chars
        linhas = len(log_texto.splitlines())
        conn.execute("""
            INSERT INTO hc_automacoes_log(motor, status, linhas, resumo, log_texto, duracao_s)
            VALUES(?,?,?,?,?,?)
        """, (motor, status, linhas, resumo, log_texto, round(duracao, 2)))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"Erro ao salvar log: {e}")


def sincronizar():
    t0 = time.time()
    log.info("=== Sync Contratos IXC iniciado ===")
    inseridos = atualizados = erros = 0

    try:
        from app.services.ixc_db import ixc_conn
        conn = get_db()

        with ixc_conn() as ixc:
            cur = ixc.cursor()
            cur.execute("""
                SELECT
                    cc.id AS contrato_id,
                    cc.id_cliente,
                    c.razao,
                    c.cnpj_cpf,
                    ci.nome AS cidade_nome,
                    c.bairro,
                    c.id_vendedor AS vendedor_id,
                    v.nome AS vendedor_nome,
                    cc.id_vd_contrato AS plano_id,
                    vc.nome AS plano_nome,
                    vc.valor_contrato AS plano_valor,
                    cc.status AS status_contrato,
                    cc.status_internet AS status_acesso,
                    cc.data AS data_contrato,
                    cc.data_ativacao,
                    o.status AS os_status,
                    f.funcionario AS os_tecnico,
                    o.data_fechamento AS os_data_fechamento
                FROM ixcprovedor.cliente_contrato cc
                JOIN ixcprovedor.cliente c ON c.id = cc.id_cliente
                LEFT JOIN ixcprovedor.cidade ci ON ci.id = c.cidade
                LEFT JOIN ixcprovedor.vendedor v ON v.id = c.id_vendedor
                LEFT JOIN ixcprovedor.vd_contratos vc ON vc.id = cc.id_vd_contrato
                LEFT JOIN ixcprovedor.su_oss_chamado o
                    ON o.id_cliente = c.id AND o.id_assunto = 227
                LEFT JOIN ixcprovedor.funcionarios f ON f.id = o.id_tecnico
                WHERE cc.data >= '2026-01-01'
                  AND c.id_vendedor > 0
                ORDER BY cc.id DESC
                LIMIT 1000
            """)
            rows = cur.fetchall()

        log.info(f"IXC retornou {len(rows)} contratos")

        for r in rows:
            try:
                existe = conn.execute(
                    "SELECT id FROM hc_contratos_cache WHERE ixc_contrato_id=?",
                    (r["contrato_id"],)
                ).fetchone()

                def sv(v):
                    if v is None: return None
                    if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal': return float(v)
                    if hasattr(v, 'isoformat'): return str(v)
                    return v

                os_fech = sv(r["os_data_fechamento"])
                if os_fech in ("0000-00-00 00:00:00", "0000-00-00"): os_fech = None

                params = (
                    r["id_cliente"], r["razao"], r["cnpj_cpf"],
                    r["cidade_nome"], r["bairro"],
                    r["vendedor_id"], r["vendedor_nome"],
                    r["plano_id"], r["plano_nome"], sv(r["plano_valor"]),
                    r["status_contrato"], r["status_acesso"],
                    sv(r["data_contrato"]), sv(r["data_ativacao"]),
                    r["os_status"], r["os_tecnico"], os_fech,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    r["contrato_id"]
                )

                if existe:
                    conn.execute("""
                        UPDATE hc_contratos_cache SET
                            ixc_cliente_id=?, razao=?, cnpj_cpf=?,
                            cidade_nome=?, bairro=?,
                            vendedor_id=?, vendedor_nome=?,
                            plano_id=?, plano_nome=?, plano_valor=?,
                            status_contrato=?, status_acesso=?,
                            data_contrato=?, data_ativacao=?,
                            os_status=?, os_tecnico=?, os_data_fechamento=?,
                            sincronizado_em=?
                        WHERE ixc_contrato_id=?
                    """, params)
                    atualizados += 1
                else:
                    conn.execute("""
                        INSERT INTO hc_contratos_cache (
                            ixc_cliente_id, razao, cnpj_cpf,
                            cidade_nome, bairro,
                            vendedor_id, vendedor_nome,
                            plano_id, plano_nome, plano_valor,
                            status_contrato, status_acesso,
                            data_contrato, data_ativacao,
                            os_status, os_tecnico, os_data_fechamento,
                            sincronizado_em, ixc_contrato_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, params)
                    inseridos += 1

            except Exception as e:
                erros += 1
                log.warning(f"Erro contrato {r.get('contrato_id')}: {e}")

        conn.commit()
        conn.close()

        duracao = time.time() - t0
        resumo = f"{inseridos} inseridos | {atualizados} atualizados | {erros} erros | {duracao:.1f}s"
        log.info(f"=== Sync concluido: {resumo} ===")
        salvar_log("Sync Contratos IXC", "ok", resumo, duracao)

    except Exception as e:
        duracao = time.time() - t0
        log.error(f"ERRO CRITICO: {e}")
        salvar_log("Sync Contratos IXC", "erro", str(e), duracao)


if __name__ == "__main__":
    sincronizar()
