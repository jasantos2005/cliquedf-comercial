"""
bootstrap/cron_churn_score.py — HubRetencao Cliquedf
Versao em lote: busca todos os dados de uma vez, muito mais rapido.
Cron: 0 6 * * * (03h BRT)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import sqlite3, json
from datetime import datetime
from app.services.ixc_db import ixc_select
from app.routes.retencao import init_retencao_tables, get_db

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "hub_comercial.db")

def run():
    inicio = datetime.now()
    print(f"\n[{inicio.strftime('%d/%m/%Y %H:%M')}] === cron_churn_score iniciado ===")
    init_retencao_tables()

    # ── 1. Contratos ativos ──────────────────────────────────────────
    contratos = ixc_select("""
        SELECT cc.id, cc.id_cliente, cc.status, cc.data_ativacao,
               cc.id_vd_contrato AS id_plano, cc.valor_unitario AS valor_plano,
               cc.fidelidade AS data_fidelidade,
               cl.razao AS nome, cl.cnpj_cpf AS cpf,
               cc.cidade, cc.bairro
        FROM cliente_contrato cc
        JOIN cliente cl ON cl.id = cc.id_cliente
        WHERE cc.status = 'A' AND cc.data_ativacao >= '2024-01-01'
    """)
    total = len(contratos)
    print(f"[INFO] {total} contratos ativos")
    if not total:
        return

    ids = [str(r["id"]) for r in contratos]
    ids_str = ",".join(ids)

    # ── 2. Planos ────────────────────────────────────────────────────
    planos_rows = ixc_select("SELECT id, nome FROM vd_contratos")
    planos = {str(r["id"]): r["nome"] for r in planos_rows}

    # ── 3. Faturas vencidas por contrato ────────────────────────────
    fat_rows = ixc_select(f"""
        SELECT id_contrato,
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) > 15 THEN 1 ELSE 0 END) AS grave,
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) BETWEEN 5 AND 15 THEN 1 ELSE 0 END) AS moderado,
          SUM(CASE WHEN DATEDIFF(CURDATE(), data_vencimento) BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS leve
        FROM fn_areceber
        WHERE id_contrato IN ({ids_str})
          AND status IN ('A','P') AND data_vencimento < CURDATE()
        GROUP BY id_contrato
    """)
    faturas = {str(r["id_contrato"]): r for r in fat_rows}

    # ── 4. Suspensoes 6 meses ───────────────────────────────────────
    susp_rows = ixc_select(f"""
        SELECT id_contrato, COUNT(*) AS total
        FROM cliente_contrato_historico
        WHERE id_contrato IN ({ids_str}) AND tipo = 'S'
          AND data >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
        GROUP BY id_contrato
    """)
    suspensoes = {str(r["id_contrato"]): int(r["total"]) for r in susp_rows}

    # ── 5. OS abertas antigas (>7 dias) ─────────────────────────────
    os_ant_rows = ixc_select(f"""
        SELECT id_contrato_kit, COUNT(*) AS total
        FROM su_oss_chamado
        WHERE id_contrato_kit IN ({ids_str}) AND status = 'A'
          AND id_assunto IN (20, 21, 16, 94, 113, 248)
          AND DATEDIFF(CURDATE(), data_abertura) > 7
        GROUP BY id_contrato_kit
    """)
    os_antigas = {str(r["id_contrato_kit"]): int(r["total"]) for r in os_ant_rows}

    # ── 6. OS no mes ────────────────────────────────────────────────
    os_mes_rows = ixc_select(f"""
        SELECT id_contrato_kit, COUNT(*) AS total
        FROM su_oss_chamado
        WHERE id_contrato_kit IN ({ids_str})
          AND id_assunto IN (20, 21, 16, 94, 113, 248)
          AND data_abertura >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
        GROUP BY id_contrato_kit
    """)
    os_mes = {str(r["id_contrato_kit"]): int(r["total"]) for r in os_mes_rows}

    # ── 7. Calcular score para cada contrato ────────────────────────
    resultados = []
    mes_atual = datetime.now().month

    for row in contratos:
        cid = str(row["id"])
        motivos = []
        pts_fin = pts_tec = pts_comp = pts_ctx = 0

        meses_casa = 0
        try:
            da = datetime.strptime(str(row["data_ativacao"])[:10], "%Y-%m-%d")
            meses_casa = (datetime.now() - da).days // 30
        except Exception:
            pass

        # Financeiro
        fat = faturas.get(cid)
        if fat:
            if fat["grave"] and int(fat["grave"]) > 0:
                pts_fin += 35
                motivos.append(f"💰 {fat['grave']} fatura(s) vencida(s) há mais de 15 dias")
            elif fat["moderado"] and int(fat["moderado"]) > 0:
                pts_fin += 20
                motivos.append("💰 Fatura vencida há 5-15 dias")
            elif fat["leve"] and int(fat["leve"]) > 0:
                pts_fin += 10
                motivos.append("💰 Fatura vencida nos últimos 4 dias")

        susp = suspensoes.get(cid, 0)
        if susp >= 3:
            pts_fin += 15
            motivos.append(f"💰 {susp} suspensões nos últimos 6 meses")
        elif susp >= 1:
            pts_fin += 7
            motivos.append(f"💰 {susp} suspensão(ões) nos últimos 6 meses")

        # Tecnico
        if os_antigas.get(cid, 0) > 0:
            pts_tec += 30
            motivos.append("🔧 OS aberta há mais de 7 dias sem resolução")

        os_m = os_mes.get(cid, 0)
        if os_m >= 3:
            pts_tec += 20
            motivos.append(f"🔧 {os_m} OS abertas este mês")
        elif os_m == 2:
            pts_tec += 10
            motivos.append("🔧 2 OS abertas este mês")

        # Comportamental
        if row["data_fidelidade"]:
            try:
                df = datetime.strptime(str(row["data_fidelidade"])[:10], "%Y-%m-%d")
                dias_fim = (df - datetime.now()).days
                if 0 < dias_fim <= 30:
                    pts_comp += 20
                    motivos.append(f"⏰ Fidelidade expira em {dias_fim} dias")
                elif 0 < dias_fim <= 60:
                    pts_comp += 12
                    motivos.append(f"⏰ Fidelidade expira em {dias_fim} dias")
            except Exception:
                pass

        if os_mes.get(cid, 0) == 0 and meses_casa > 12:
            pts_comp += 10
            motivos.append("👻 Cliente sem interação nos últimos 12 meses")

        # Contextual
        if mes_atual in (1, 2):
            pts_ctx += 8
            motivos.append("📅 Período sazonal de alto cancelamento")

        # Protecao
        bonus = 0
        if meses_casa > 24:
            bonus = -10
            motivos.append("✅ Cliente fiel há mais de 2 anos (-10 pts)")
        elif meses_casa > 12:
            bonus = -5

        pts_fin  = min(pts_fin, 35)
        pts_tec  = min(pts_tec, 30)
        pts_comp = min(pts_comp, 20)
        pts_ctx  = min(pts_ctx, 15)
        score = max(0, min(100, pts_fin + pts_tec + pts_comp + pts_ctx + bonus))
        faixa = "alto" if score >= 40 else ("medio" if score >= 20 else "baixo")

        if not [m for m in motivos if not m.startswith("✅")]:
            script = "✅ Cliente sem fatores de risco. Ligação de relacionamento."
        elif pts_fin >= pts_tec:
            script = "💰 Foco financeiro: ofereça negociação de débito ou desconto pontual."
        elif pts_tec > pts_comp:
            script = "🔧 Foco técnico: comprometa-se com resolução definitiva. Ofereça técnico prioritário."
        elif pts_comp >= 15:
            script = "⏰ Fidelidade próxima do fim: ofereça renovação com benefícios."
        else:
            script = "📋 Retenção preventiva: escute o cliente e ofereça benefício surpresa."

        resultados.append((
            row["id"], row["id_cliente"], row["nome"], row["cpf"],
            row["cidade"] or "", row["bairro"] or "",
            planos.get(str(row["id_plano"]), "Desconhecido"),
            float(row["valor_plano"] or 0), row["status"],
            str(row["data_ativacao"])[:10] if row["data_ativacao"] else "",
            score, faixa, pts_fin, pts_tec, pts_comp, pts_ctx,
            json.dumps(motivos, ensure_ascii=False), script,
            datetime.now().strftime("%d/%m/%Y %H:%M")
        ))

    # ── 8. Salvar no SQLite ──────────────────────────────────────────
    db = get_db()
    db.executemany("""
        INSERT INTO hc_churn_score
            (ixc_contrato_id, ixc_cliente_id, cliente_nome, cpf, cidade, bairro,
             plano_nome, plano_valor, status_contrato, data_ativacao,
             score, faixa, pts_financeiro, pts_tecnico, pts_comportamental, pts_contextual,
             motivos, script_sugerido, calculado_em)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(ixc_contrato_id) DO UPDATE SET
            score=excluded.score, faixa=excluded.faixa,
            pts_financeiro=excluded.pts_financeiro, pts_tecnico=excluded.pts_tecnico,
            pts_comportamental=excluded.pts_comportamental, pts_contextual=excluded.pts_contextual,
            motivos=excluded.motivos, script_sugerido=excluded.script_sugerido,
            calculado_em=excluded.calculado_em,
            plano_nome=excluded.plano_nome, status_contrato=excluded.status_contrato
    """, resultados)
    db.commit()
    db.close()

    duracao = (datetime.now() - inicio).total_seconds()
    alto = sum(1 for r in resultados if r[11] == "alto")
    medio = sum(1 for r in resultados if r[11] == "medio")
    resumo = f"Concluido | {len(resultados)}/{total} | Alto: {alto} | Medio: {medio} | {duracao:.0f}s"
    print(resumo)

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO hc_automacoes_log (motor, status, linhas, resumo, duracao_s) VALUES (?,?,?,?,?)",
                     ('cron_churn_score', 'ok', len(resultados), resumo, duracao))
        conn.commit()
        conn.close()
    except Exception:
        pass

if __name__ == "__main__":
    run()
