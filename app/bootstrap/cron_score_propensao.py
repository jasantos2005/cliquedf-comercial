"""
cron_score_propensao.py
Calcula score de propensão para alteração de plano.
Roda diariamente às 07h30.

Critérios:
  +30  Tempo de casa > 3 anos
  +15  Tempo de casa 1-3 anos
  +25  Zero parcelas em atraso
  +20  Situação financeira Regular (R)
  +10  Renovação automática S
  +15  Plano <= R$75 (maior margem de upgrade)
  -30  Já recusou anteriormente

Faixas:
  71-100 → alta  🟢
  41-70  → media 🟡
  0-40   → baixa 🔴
"""
import sqlite3, os, sys, logging
from pathlib import Path
from datetime import datetime, date

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

DB_PATH = BASE_DIR / "hub_comercial.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def calcular_score(data_contrato, num_atraso, situacao_fin, renovacao_auto, valor_plano, recusou):
    score = 0
    hoje = date.today()

    # Tempo de casa
    if data_contrato:
        anos = (hoje - data_contrato).days / 365
        if anos >= 3:
            score += 30
        elif anos >= 1:
            score += 15

    # Adimplência
    if (num_atraso or 0) == 0:
        score += 25

    # Situação financeira
    if situacao_fin == 'R':
        score += 20

    # Renovação automática
    if renovacao_auto == 'S':
        score += 10

    # Margem de upgrade (planos mais baratos aceitam mais)
    if (valor_plano or 0) <= 75:
        score += 15

    # Penalidade por recusa anterior
    if recusou:
        score -= 30

    return max(0, min(100, score))

def faixa(score):
    if score >= 71: return "alta"
    if score >= 41: return "media"
    return "baixa"

def main():
    from app.services.ixc_db import ixc_conn

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Busca contratos pendentes/em_contato da alteracao_planos
    contratos_hub = conn.execute("""
        SELECT ixc_contrato_id, status_alteracao
        FROM hc_alteracao_planos
        WHERE status_alteracao NOT IN ('alterado', 'recusou')
    """).fetchall()

    # Busca também todos os contratos ativos futuros que ainda não estão no hub
    from app.services.ixc_db import ixc_conn as _ixc

    ids_hub = [r["ixc_contrato_id"] for r in contratos_hub]
    recusaram = set(
        r["ixc_contrato_id"] for r in conn.execute(
            "SELECT ixc_contrato_id FROM hc_alteracao_planos WHERE status_alteracao='recusou'"
        ).fetchall()
    )

    agora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    atualizados = 0

    with _ixc() as ixc:
        with ixc.cursor() as cur:
            cur.execute("""
                SELECT cc.id, cc.data, cc.num_parcelas_atraso,
                       cc.situacao_financeira_contrato, cc.renovacao_automatica,
                       vd.valor_contrato
                FROM cliente_contrato cc
                INNER JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
                WHERE cc.status = 'A'
                  AND cc.data_expiracao >= CURDATE()
                  AND cc.data_expiracao != '0000-00-00'
            """)
            contratos_ixc = {r["id"]: r for r in cur.fetchall()}

    for cid, row in contratos_ixc.items():
        recusou = cid in recusaram
        score = calcular_score(
            data_contrato   = row["data"],
            num_atraso      = row["num_parcelas_atraso"],
            situacao_fin    = row["situacao_financeira_contrato"],
            renovacao_auto  = row["renovacao_automatica"],
            valor_plano     = float(row["valor_contrato"] or 0),
            recusou         = recusou,
        )
        fx = faixa(score)

        existe = conn.execute(
            "SELECT id FROM hc_alteracao_planos WHERE ixc_contrato_id=?", (cid,)
        ).fetchone()

        if existe:
            conn.execute("""
                UPDATE hc_alteracao_planos
                SET score=?, score_faixa=?, score_calculado_em=?
                WHERE ixc_contrato_id=?
            """, (score, fx, agora_str, cid))
        else:
            conn.execute("""
                INSERT INTO hc_alteracao_planos
                    (ixc_contrato_id, score, score_faixa, score_calculado_em,
                     status_alteracao, criado_em, atualizado_em)
                VALUES (?,?,?,?,'pendente',?,?)
            """, (cid, score, fx, agora_str, agora_str, agora_str))
        atualizados += 1

    conn.commit()
    conn.close()
    log.info(f"Score calculado para {atualizados} contratos.")

if __name__ == "__main__":
    main()
