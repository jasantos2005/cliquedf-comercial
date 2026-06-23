import sys
sys.path.insert(0, ".")
from app.services.ixc_db import ixc_conn

MESES_PT = {
    "01": "Janeiro",  "02": "Fevereiro", "03": "Março",
    "04": "Abril",    "05": "Maio",       "06": "Junho",
    "07": "Julho",    "08": "Agosto",     "09": "Setembro",
    "10": "Outubro",  "11": "Novembro",   "12": "Dezembro",
}

SQL = """
    SELECT
        CASE
            WHEN cc.data_expiracao IS NULL
              OR cc.data_expiracao = '0000-00-00' THEN 'sem-data'
            ELSE DATE_FORMAT(cc.data_expiracao, '%Y-%m')
        END                                       AS mes_chave,
        COUNT(*)                                  AS total,
        MIN(cc.data_expiracao)                    AS primeiro,
        MAX(cc.data_expiracao)                    AS ultimo,
        SUM(vd.valor_contrato)                    AS receita_total
    FROM
        cliente_contrato cc
        INNER JOIN cliente c       ON c.id  = cc.id_cliente
        INNER JOIN vd_contratos vd ON vd.id = cc.id_vd_contrato
    WHERE
        cc.status = 'A'
    GROUP BY
        mes_chave
    ORDER BY
        mes_chave ASC
"""

print("\n📅  CONTRATOS POR MÊS DE VENCIMENTO — TODOS OS ATIVOS")
print("=" * 62)
print(f"{'Mês':<20} {'Contratos':>10} {'Receita':>16}  {'Período'}")
print("-" * 62)

total_contratos = 0
total_receita   = 0.0

with ixc_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(SQL)
        rows = cur.fetchall()

if not rows:
    print("⚠️  Nenhum contrato encontrado.")
else:
    for row in rows:
        chave = row["mes_chave"]
        if chave == "sem-data":
            label = "⚠️  Sem data"
        else:
            ano, mes_num = chave.split("-")
            label = f"{MESES_PT[mes_num]}/{ano}"
        total   = row["total"]
        receita = float(row["receita_total"] or 0)
        periodo = f"{row['primeiro']}  →  {row['ultimo']}"
        total_contratos += total
        total_receita   += receita
        print(f"{label:<20} {total:>10}    R$ {receita:>12,.2f}  {periodo}")

    print("=" * 62)
    print(f"{'TOTAL':<20} {total_contratos:>10}    R$ {total_receita:>12,.2f}")

print()
