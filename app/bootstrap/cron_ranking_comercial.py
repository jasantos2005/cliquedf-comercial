"""
Hub Comercial — cron_ranking_comercial.py
==========================================
Horários:
  08:00 — Abertura: meta do dia + resumo do dia anterior
  09h-11h, 13h-17h — Atualização horária
  12:00 — Resumo da manhã
  18:00 — Fechamento do dia + resumo da semana
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def notificar(msg, chat_id=None):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id or TELEGRAM_GRUPO, "text": msg, "parse_mode": "Markdown"},
            timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")


def db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def ranking_vendedores(data_inicio, data_fim=None):
    """Retorna ranking de vendedores no periodo."""
    conn = db()
    if not data_fim:
        data_fim = datetime.now().strftime("%Y-%m-%d 23:59:59")

    rows = conn.execute("""
        SELECT v.nome,
               SUM(CASE WHEN p.status='ativado' THEN 1 ELSE 0 END) as ativados,
               SUM(CASE WHEN p.status='reprovado' THEN 1 ELSE 0 END) as reprovados,
               SUM(CASE WHEN p.status='erro_ativacao' THEN 1 ELSE 0 END) as erros,
               COUNT(p.id) as total
        FROM hc_vendedores v
        LEFT JOIN hc_precadastros p ON p.ixc_vendedor_id = v.id
            AND p.criado_em >= ? AND p.criado_em <= ?
        WHERE v.ativo=1
        AND v.id IN (SELECT DISTINCT ixc_funcionario_id FROM hc_usuarios WHERE ixc_funcionario_id IS NOT NULL)
        GROUP BY v.id, v.nome
        ORDER BY ativados DESC, v.nome
    """, (data_inicio, data_fim)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def total_semana():
    """Total de ativações da semana atual (seg-hoje)."""
    hoje = datetime.now()
    seg = hoje - timedelta(days=hoje.weekday())
    seg_str = seg.strftime("%Y-%m-%d 00:00:00")
    conn = db()
    r = conn.execute("""
        SELECT SUM(status='ativado') as ativados,
               SUM(status='reprovado') as reprovados
        FROM hc_precadastros
        WHERE criado_em >= ?
    """, (seg_str,)).fetchone()
    conn.close()
    return int(r['ativados'] or 0), int(r['reprovados'] or 0)


def formatar_ranking(rows, titulo, mostrar_meta=True, mostrar_fechamento=False):
    medalhas = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣"]
    linhas = [titulo, ""]
    posicao = 0
    for r in rows:
        if r['total'] == 0 and not mostrar_fechamento:
            continue
        med = medalhas[posicao] if posicao < len(medalhas) else "▪️"
        posicao += 1
        ativ = r['ativados']
        reprov = r['reprovados'] + r['erros']
        nome = r['nome'].split()[0].title()  # Primeiro nome

        if mostrar_fechamento:
            if ativ >= META_DIA:
                status = "✅ *META ATINGIDA! Parabéns!*"
            elif ativ >= META_DIA / 2:
                status = "⚠️ Ficou pela metade, vamos melhorar!"
            elif ativ > 0:
                status = "❌ Abaixo da meta, vamos lá!"
            else:
                status = "❌ Sem ativações hoje"
            linha = f"{med} *{nome}* — {ativ} ativado(s) | {reprov} não ativado(s)\n   {status}"
        else:
            nao_ativ = f" | {reprov} não ativ." if reprov > 0 else ""
            meta_txt = f" | meta: {META_DIA}" if mostrar_meta else ""
            linha = f"{med} *{nome}* — {ativ} ativado(s){nao_ativ}{meta_txt}"
        linhas.append(linha)

    if posicao == 0:
        linhas.append("Nenhuma ativação ainda hoje.")
    return "\n".join(linhas)


def msg_abertura():
    """08:00 — Bom dia + meta + resumo de ontem."""
    hoje = datetime.now()
    ontem = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
    data_fmt = hoje.strftime("%d/%m/%Y")

    rows_ontem = ranking_vendedores(f"{ontem} 00:00:00", f"{ontem} 23:59:59")
    total_ontem = sum(r['ativados'] for r in rows_ontem)
    sem_ativ, sem_reprov = total_semana()

    ranking_txt = formatar_ranking(rows_ontem,
        f"📊 *Resultado de ontem ({(hoje-timedelta(days=1)).strftime('%d/%m')}):*",
        mostrar_meta=False, mostrar_fechamento=True)

    msg = (
        f"☀️ *Bom dia, equipe!*\n"
        f"📅 *{data_fmt}* — Meta do dia: *{META_DIA} ativações* por vendedor\n\n"
        f"{ranking_txt}\n\n"
        f"📈 *Semana atual:* {sem_ativ} ativações | {sem_reprov} não ativados\n\n"
        f"💪 Vamos superar hoje! Bora equipe! 🚀"
    )
    return msg


def msg_horaria(hora):
    """09h-11h, 13h-17h — Atualização horária."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m")
    rows = ranking_vendedores(f"{hoje} 00:00:00")
    total_ativ = sum(r['ativados'] for r in rows)

    titulo = f"📊 *Vendas do dia — {data_fmt} {hora:02d}:00*"
    ranking_txt = formatar_ranking(rows, titulo)

    msg = f"{ranking_txt}\n\n🎯 Total do dia: *{total_ativ}* ativações"
    return msg


def msg_meio_dia():
    """12:00 — Resumo da manhã."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m")
    rows = ranking_vendedores(f"{hoje} 00:00:00")
    total_ativ = sum(r['ativados'] for r in rows)
    total_reprov = sum(r['reprovados'] + r['erros'] for r in rows)
    sem_ativ, _ = total_semana()

    titulo = f"☀️ *Resumo da manhã — {data_fmt}*"
    ranking_txt = formatar_ranking(rows, titulo, mostrar_meta=True)

    msg = (
        f"{ranking_txt}\n\n"
        f"📋 Total manhã: *{total_ativ}* ativados | *{total_reprov}* não ativados\n"
        f"📅 Semana: *{sem_ativ}* ativações\n\n"
        f"💪 Tarde ainda tem! Vamos nessa! 🚀"
    )
    return msg


def msg_fechamento():
    """18:00 — Fechamento do dia + semana."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    data_fmt = datetime.now().strftime("%d/%m")
    rows = ranking_vendedores(f"{hoje} 00:00:00")
    total_ativ = sum(r['ativados'] for r in rows)
    total_reprov = sum(r['reprovados'] + r['erros'] for r in rows)
    sem_ativ, sem_reprov = total_semana()

    titulo = f"🔔 *Fechamento do dia — {data_fmt}*"
    ranking_txt = formatar_ranking(rows, titulo, mostrar_meta=True, mostrar_fechamento=True)

    msg = (
        f"{ranking_txt}\n\n"
        f"📋 *Total do dia:* {total_ativ} ativados | {total_reprov} não ativados\n"
        f"📅 *Semana (seg-hoje):* {sem_ativ} ativações | {sem_reprov} não ativados\n\n"
        f"✨ Até amanhã, equipe! Bom descanso! 🌙"
    )
    return msg


def main():
    hora = datetime.now().hour
    log.info(f"Ranking comercial — hora={hora}")

    if hora == 8:
        msg = msg_abertura()
    elif hora == 12:
        msg = msg_meio_dia()
    elif hora == 18:
        msg = msg_fechamento()
    elif 9 <= hora <= 17:
        msg = msg_horaria(hora)
    else:
        log.info("Fora do horario comercial.")
        return

    notificar(msg)
    log.info("Mensagem enviada.")


if __name__ == "__main__":
    main()
