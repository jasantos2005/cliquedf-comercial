#!/usr/bin/env python3
"""
Cron OPA Sync — roda diariamente às 6h
Sincroniza atendimentos do OPA e alerta clientes ativos com muitos chamados de suporte
"""
import sys, os, sqlite3, httpx, json, time, requests
from datetime import datetime, timezone, timedelta, date

TZ_BR = timezone(timedelta(hours=-3))
def now_br(): return datetime.now(TZ_BR)
def log(msg): print(f"[{now_br().strftime('%d/%m/%Y %H:%M:%S')}] {msg}", flush=True)

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
DB_PATH   = '/opt/automacoes/cliquedf/comercial/hub_comercial.db'

TELEGRAM_TOKEN = "7526159263:AAF-G5Y_lnjFJthSfPnbajk0gDaXXOUrmzA"
TELEGRAM_CHAT  = "-1003875285904"

DEPTOS = {
    '5bf73d1d186f7d2b0d647a60': 'Comercial',
    '5bf73d1d186f7d2b0d647a61': 'Suporte',
    '5bf73d1d186f7d2b0d647a64': 'Ag. Virtual',
    '5d1623f35e74a002308aa25d': 'Agendamentos',
    '5d1624085e74a002308aa25e': 'Financeiro',
    '5d1629315e74a002308aa262': 'Renegociacoes',
    '6a354a6cbe38146980adb7b8': 'Upgrade',
    '6a4467fd49052037b3f9aeb3': 'Atendimentos - diversos',
}

def telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        log(f"[TELEGRAM ERRO] {e}")

def buscar_dia(data_str):
    payload = {"filter": {"dataInicialAbertura": data_str, "dataFinalAbertura": data_str}, "options": {"limit": 500}}
    try:
        r = httpx.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode(),
            timeout=20
        )
        data = r.json()
        return data.get('data', data) if isinstance(data, dict) else data
    except Exception as e:
        log(f"  ERRO {data_str}: {e}")
        return []

def sync_dia(cur, data_str):
    atendimentos = buscar_dia(data_str)
    if not atendimentos:
        return 0
    total = 0
    for a in atendimentos:
        atend_id  = a.get('_id') or a.get('id','')
        protocolo = a.get('protocol','') or a.get('protocolo','')
        canal     = (a.get('customerChannel') or a.get('canal_cliente','')).replace('@c.us','')
        setor_id  = a.get('setor','') or a.get('department','')
        setor     = DEPTOS.get(setor_id, setor_id)
        status    = a.get('status','')
        atendente = a.get('agentName','') or a.get('nome_atendente','')
        id_atend  = a.get('agentId','') or a.get('id_atendente','')
        motivo    = a.get('subject','') or a.get('motivo','')
        cur.execute("""
            INSERT INTO opa_atendimentos
                (atend_id, protocolo, canal_cliente, id_atendente, nome_atendente,
                 setor, status, data_abertura, motivo, atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(atend_id) DO UPDATE SET
                status=excluded.status,
                setor=CASE WHEN excluded.setor NOT LIKE '5%' AND excluded.setor NOT LIKE '6%' THEN excluded.setor ELSE setor END,
                data_abertura=COALESCE(NULLIF(excluded.data_abertura,''), data_abertura),
                nome_atendente=COALESCE(NULLIF(excluded.nome_atendente,''), nome_atendente)
        """, (atend_id, protocolo, canal, id_atend, atendente, setor, status, data_str, motivo, data_str))
        total += 1
    return total

def alertar_clientes_risco():
    """Alerta clientes com muitos chamados de suporte ou financeiro no OPA"""
    sys.path.insert(0, '/opt/automacoes/cliquedf/cobranca')
    os.chdir('/opt/automacoes/cliquedf/cobranca')
    from app.core.db import query
    import re

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    data_limite = (now_br() - timedelta(days=30)).strftime('%Y-%m-%d')

    cur.execute("SELECT canal_cliente, COUNT(*) AS total FROM opa_atendimentos WHERE setor='Suporte' AND data_abertura >= ? GROUP BY canal_cliente HAVING total >= 3 ORDER BY total DESC", (data_limite,))
    alto_suporte = [(dict(r), "Suporte") for r in cur.fetchall()]

    cur.execute("SELECT canal_cliente, COUNT(*) AS total FROM opa_atendimentos WHERE setor='Financeiro' AND data_abertura >= ? GROUP BY canal_cliente HAVING total >= 2 ORDER BY total DESC", (data_limite,))
    alto_fin = [(dict(r), "Financeiro") for r in cur.fetchall()]
    conn.close()

    def fmt_fone(canal):
        f = re.sub(r"[^0-9]", "", canal)
        if f.startswith("55"): f = f[2:]
        if len(f) == 11: return f"({f[:2]}) {f[2:7]}-{f[7:]}"
        if len(f) == 10: return f"({f[:2]}) {f[2:6]}-{f[6:]}"
        return f

    alertas_sup, alertas_fin, vistos = [], [], set()
    for row, tipo in alto_suporte + alto_fin:
        fmt = fmt_fone(row["canal_cliente"])
        if fmt in vistos: continue
        cli = query("SELECT c.id, c.razao FROM ixcprovedor.cliente c INNER JOIN ixcprovedor.cliente_contrato cc ON cc.id_cliente=c.id AND cc.status='A' WHERE c.whatsapp=%s OR c.telefone_celular=%s OR c.fone=%s LIMIT 1", (fmt, fmt, fmt))
        if cli:
            vistos.add(fmt)
            item = {"razao": cli[0]["razao"], "qtd": row["total"], "canal": fmt}
            if tipo == "Suporte": alertas_sup.append(item)
            else: alertas_fin.append(item)

    if not alertas_sup and not alertas_fin:
        log("Nenhum cliente em risco")
        return

    linhas = ["🚨 <b>RISCO DE CANCELAMENTO — Chamados OPA</b>", ""]
    if alertas_sup:
        linhas.append("🔧 <b>Alto suporte técnico (3+ chamados/30d):</b>")
        for a in alertas_sup[:8]:
            linhas.append(f"  • <b>{a['razao']}</b> — {a['qtd']}x suporte ({a['canal']})")
        linhas.append("")
    if alertas_fin:
        linhas.append("💰 <b>Dificuldade financeira (2+ chamados financeiro/30d):</b>")
        for a in alertas_fin[:8]:
            linhas.append(f"  • <b>{a['razao']}</b> — {a['qtd']}x financeiro ({a['canal']})")
        linhas.append("")
    linhas.append("💡 <b>Ação:</b> Entrar em contato proativamente para retenção")
    linhas.append(f"\n<i>IaTechHub · {now_br().strftime('%d/%m/%Y %H:%M')}</i>")
    telegram("\n".join(linhas))
    log(f"Alerta: {len(alertas_sup)} suporte | {len(alertas_fin)} financeiro")

def main():
    log("=== SYNC OPA ===")
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # Busca último dia sincronizado
    cur.execute("SELECT MAX(data_abertura) FROM opa_atendimentos WHERE data_abertura IS NOT NULL AND data_abertura != ''")
    ultimo = cur.fetchone()[0] or '2026-01-01'
    log(f"Último sync: {ultimo}")

    # Sincroniza do dia seguinte até hoje
    from datetime import date as dt
    d_ini = datetime.strptime(ultimo, '%Y-%m-%d').date() + timedelta(days=1)
    d_fim = now_br().date()
    total = 0

    d = d_ini
    while d <= d_fim:
        ds = d.strftime('%Y-%m-%d')
        qtd = sync_dia(cur, ds)
        if qtd:
            log(f"  {ds}: {qtd} atendimentos")
            total += qtd
        conn.commit()
        time.sleep(0.3)
        d += timedelta(days=1)

    conn.close()
    log(f"Sync concluído: {total} novos atendimentos")

    # Alerta clientes em risco
    alertar_clientes_risco()

if __name__ == "__main__":
    main()
