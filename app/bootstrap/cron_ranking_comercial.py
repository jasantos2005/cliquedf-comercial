"""
Hub Comercial — cron_ranking_comercial.py
Horários seg-sex:
  08:00 — Abertura: meta + resumo de ontem
  09h-11h, 14h-17h — Atualização horária
  12:00 — Resumo da manhã detalhado
  18:00 — Fechamento detalhado + semana
"""
import sqlite3, logging, os, sys, requests
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

DB_PATH        = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_GRUPO = os.getenv("TELEGRAM_CHAT_ID", "")
META_DIA       = 4
VENDEDORES_IDS = (31, 45, 48, 6, 49, 22)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def notificar(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_GRUPO, "text": msg, "parse_mode": "Markdown"},
            timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")


def get_db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def ranking_vendedores(data_inicio, data_fim=None):
    if not data_fim:
        data_fim = datetime.now().strftime("%Y-%m-%d 23:59:59")
    conn = get_db()
    rows = conn.execute("""
        SELECT v.id, v.nome,
               SUM(CASE WHEN p.status='ativado' THEN 1 ELSE 0 END) as ativados,
               SUM(CASE WHEN p.status='reprovado' THEN 1 ELSE 0 END) as reprovados,
               SUM(CASE WHEN p.status='erro_ativacao' THEN 1 ELSE 0 END) as erros,
               COUNT(p.id) as total
        FROM hc_vendedores v
        LEFT JOIN hc_precadastros p ON p.ixc_vendedor_id = v.id
            AND p.criado_em >= ? AND p.criado_em <= ?
        WHERE v.ativo=1 AND v.id IN (31,45,48,6,49,22)
        GROUP BY v.id, v.nome
        ORDER BY ativados DESC, v.nome
    """, (data_inicio, data_fim)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def clientes_do_vendedor(vendedor_id, data_inicio):
    conn = get_db()
    rows = conn.execute("""
        SELECT razao, cidade_nome, plano_nome, status
        FROM hc_precadastros
        WHERE ixc_vendedor_id=? AND criado_em >= ?
        AND status IN ('ativado','aprovado','assinatura_pendente','reprovado','erro_ativacao')
        ORDER BY criado_em DESC
    """, (vendedor_id, data_inicio)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def total_semana():
    hoje = datetime.now()
    seg = hoje - timedelta(days=hoje.weekday())
    seg_str = seg.strftime("%Y-%m-%d 00:00:00")
    conn = get_db()
    r = conn.execute("""
        SELECT SUM(status='ativado') as ativados,
               SUM(status='reprovado') as reprovados
        FROM hc_precadastros WHERE criado_em >= ?
    """, (seg_str,)).fetchone()
    conn.close()
    return int(r['ativados'] or 0), int(r['reprovados'] or 0)


def bloco_vendedor_simples(r, medalha, mostrar_clientes=False, data_inicio=None):
    nome = r['nome'].split()[0].title()
    ativ = r['ativados']
    reprov = r['reprovados'] + r['erros']
    linhas = [f"{medalha} *{nome}* — {ativ} ativado(s) | {reprov} nao ativ. | meta: {META_DIA}"]
    return "\n".join(linhas)


def bloco_vendedor_detalhado(r, medalha, data_inicio):
    nome = r['nome'].split()[0].title()
    ativ = r['ativados']
    reprov = r['reprovados'] + r['erros']
    pct = int(ativ / META_DIA * 100)
    barra = "🟩" * min(ativ, META_DIA) + "⬜" * max(0, META_DIA - ativ)

    if ativ >= META_DIA:
        status_meta = "✅ *META ATINGIDA\\! Parabens\\!* 🎉"
    elif ativ >= META_DIA / 2:
        status_meta = "⚠️ Ficou pela metade"
    elif ativ > 0:
        status_meta = "❌ Abaixo da meta"
    else:
        status_meta = "❌ Sem ativacoes hoje"

    linhas = [
        f"{medalha} *{nome}* — {status_meta}",
        f"   {barra} {pct}% \\| ✅ {ativ} ativ\\. \\| ❌ {reprov} nao ativ\\.",
    ]

    clientes = clientes_do_vendedor(r['id'], data_inicio)
    ativados = [cl for cl in clientes if cl['status'] == 'ativado']
    if ativados:
        for cl in ativados:
            nome_cli = ' '.join(cl['razao'].split()[:2]).title() if cl['razao'] else '?'
            cidade = cl.get('cidade_nome') or ''
            plano = (cl.get('plano_nome') or '').replace('CLIQUEDF - 2026 ','').replace('CLIQUEDF - 2026','').strip()
            linhas.append(f"   └ {nome_cli} · {cidade} · {plano}")
    return "\n".join(linhas)


def msg_abertura():
    hoje = datetime.now()
    ontem = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
    data_fmt = hoje.strftime("%d/%m/%Y")
    rows_ontem = ranking_vendedores(f"{ontem} 00:00:00", f"{ontem} 23:59:59")
    sem_ativ, _ = total_semana()
    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]

    linhas = [
        "☀️ *BOM DIA, EQUIPE\\!*",
        f"📅 {data_fmt} \\| Meta: *{META_DIA} ativacoes* por vendedor",
        "",
        "*📊 Resultado de ontem:*",
    ]
    for i, r in enumerate(rows_ontem):
        med = medalhas[i] if i < len(medalhas) else "▪️"
        nome = r['nome'].split()[0].title()
        ativ = r['ativados']
        status = "✅ Meta\\!" if ativ >= META_DIA else f"{ativ} ativ\\."
        linhas.append(f"{med} *{nome}* — {status}")

    linhas += [
        "",
        f"📅 Semana: *{sem_ativ}* ativacoes",
        "",
        "💪 Vamos superar hoje\\! Bora equipe\\! 🚀",
    ]
    return "\n".join(linhas)


def msg_horaria(hora):
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m")
    rows = ranking_vendedores(f"{hoje} 00:00:00")
    total_ativ = sum(r['ativados'] for r in rows)
    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]

    linhas = [f"📊 *Vendas do dia — {data_fmt}*", ""]
    for i, r in enumerate(rows):
        med = medalhas[i] if i < len(medalhas) else "▪️"
        linhas.append(bloco_vendedor_simples(r, med))

    linhas += ["", f"🎯 Total: *{total_ativ}* ativacoes"]
    return "\n".join(linhas)


def msg_meio_dia():
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m/%Y")
    data_inicio = f"{hoje} 00:00:00"
    rows = ranking_vendedores(data_inicio)
    total_ativ  = sum(r['ativados'] for r in rows)
    total_reprov = sum(r['reprovados'] + r['erros'] for r in rows)
    sem_ativ, _ = total_semana()
    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]

    linhas = [
        "━━━━━━━━━━━━━━━━━━━━",
        "☀️ *RESUMO DA MANHA*",
        f"📅 {data_fmt}",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🎯 Meta: *{META_DIA}* ativacoes por vendedor",
        "",
        "*📊 RANKING:*",
        "",
    ]
    for i, r in enumerate(rows):
        med = medalhas[i] if i < len(medalhas) else "▪️"
        linhas.append(bloco_vendedor_detalhado(r, med, data_inicio))
        linhas.append("")

    linhas += [
        "━━━━━━━━━━━━━━━━━━━━",
        f"📋 Total manha: *{total_ativ}* ativ\\. \\| *{total_reprov}* nao ativ\\.",
        f"📅 Semana: *{sem_ativ}* ativacoes",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        "💪 Tarde ainda tem\\! Vamos nessa\\! 🚀",
    ]
    return "\n".join(linhas)


def msg_fechamento():
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m/%Y")
    data_inicio = f"{hoje} 00:00:00"
    rows = ranking_vendedores(data_inicio)
    total_ativ  = sum(r['ativados'] for r in rows)
    total_reprov = sum(r['reprovados'] + r['erros'] for r in rows)
    sem_ativ, sem_reprov = total_semana()
    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]

    linhas = [
        "━━━━━━━━━━━━━━━━━━━━",
        "🔔 *FECHAMENTO DO DIA*",
        f"📅 {data_fmt}",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🎯 Meta: *{META_DIA}* ativacoes por vendedor",
        "",
        "*🏆 RESULTADO FINAL:*",
        "",
    ]
    for i, r in enumerate(rows):
        med = medalhas[i] if i < len(medalhas) else "▪️"
        linhas.append(bloco_vendedor_detalhado(r, med, data_inicio))
        linhas.append("")

    linhas += [
        "━━━━━━━━━━━━━━━━━━━━",
        f"📋 *Total do dia:* {total_ativ} ativ\\. \\| {total_reprov} nao ativ\\.",
        "",
        f"📅 *Semana \\(seg\\-hoje\\):*",
        f"   ✅ {sem_ativ} ativacoes \\| ❌ {sem_reprov} nao ativados",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        "✨ Ate amanha, equipe\\! Bom descanso\\! 🌙",
    ]
    return "\n".join(linhas)


def main():
    hora = datetime.now().hour
    log.info(f"Ranking comercial — hora={hora}")

    if hora == 8:
        msg = msg_abertura()
    elif hora == 12:
        msg = msg_meio_dia()
    elif hora == 18:
        msg = msg_fechamento()
    elif hora in (9,10,11,14,15,16,17):
        msg = msg_horaria(hora)
    else:
        log.info("Fora do horario comercial.")
        return

    notificar(msg)
    log.info("Mensagem enviada.")


if __name__ == "__main__":
    main()
