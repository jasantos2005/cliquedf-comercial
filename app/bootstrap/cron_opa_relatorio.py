"""
Cron OPA Relatório Diário — roda todo dia às 7h
Envia resumo do dia anterior no Telegram GESTÃO | COMERCIAL
"""
import httpx, json, asyncio
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))

NOMES = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '5d1642ad4b16a50312cc8f4d': 'Caique (bot)',
}
DEPTOS = {
    '5bf73d1d186f7d2b0d647a61': 'Suporte',
    '5bf73d1d186f7d2b0d647a60': 'Comercial',
    '5d1624085e74a002308aa25e': 'Financeiro',
    '5bf73d1d186f7d2b0d647a64': 'Ag. Virtual',
    '5d1629315e74a002308aa262': 'Renegociações',
}
MOTIVOS = {
    '65a18e3bae4972531a90d0a1': 'Resolvido no atend.',
    '665a205084d5f75ec0b077de': 'Falta de comunicação',
    '65a18e11ae4972531a90d06d': '2ª via de boleto',
    '65a18d45f2a21eee31c88395': 'Comprovante pgto.',
    '65a18e4ef2a21eee31c88491': 'Orientação/Dúvida',
    '65a18da2ae4972531a90d014': 'Dúv. visita técnica',
    '65a18e2ef2a21eee31c8846e': 'Transf. outro setor',
    '65a18d38ae4972531a90cfae': 'Central do cliente',
    '65a18d55ae4972531a90cfd3': 'Promessa de pgto.',
    '6643c64684d5f75ec0a9155a': 'Verificar conexão',
}


async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})


async def buscar(data_str: str):
    payload = {"filter": {"dataInicialAbertura": data_str, "dataFinalAbertura": data_str}, "options": {"limit": 500}}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode()
        )
    return r.json().get('data', [])


async def main():
    agora   = datetime.now(BRT)
    ontem   = agora - timedelta(days=1)
    data_str = ontem.strftime('%Y-%m-%d')
    dia_fmt  = ontem.strftime('%d/%m/%Y')
    dia_sem  = ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'][ontem.weekday()]

    atends = await buscar(data_str)
    total  = len(atends)
    if total == 0:
        await telegram(f'📊 *RELATÓRIO {dia_fmt}*\nNenhum atendimento registrado.')
        return

    fin  = [a for a in atends if a.get('status') == 'F']
    ea   = [a for a in atends if a.get('status') == 'EA']
    sem  = [a for a in atends if not a.get('id_atendente')]
    pct  = round(len(fin) / total * 100)

    # Tempo médio
    tempos = []
    for a in fin:
        try:
            m = (datetime.fromisoformat(a['fim'].replace('Z','+00:00')) -
                 datetime.fromisoformat(a['date'].replace('Z','+00:00'))).total_seconds() / 60
            if m > 0: tempos.append(m)
        except: pass
    t_medio = round(sum(tempos)/len(tempos)) if tempos else 0
    t_max   = round(max(tempos)) if tempos else 0

    # Ranking
    rank = defaultdict(lambda: {'total':0,'fin':0})
    for a in atends:
        if not a.get('id_atendente'): continue
        rank[a['id_atendente']]['total'] += 1
        if a.get('status') == 'F': rank[a['id_atendente']]['fin'] += 1
    rank_sorted = sorted(rank.items(), key=lambda x: x[1]['total'], reverse=True)[:5]

    # Motivos top 5
    motivos_cnt = defaultdict(int)
    for a in atends:
        for m in a.get('motivos', []):
            motivos_cnt[m.get('idMotivo','')] += 1
    motivos_top = sorted(motivos_cnt.items(), key=lambda x: x[1], reverse=True)[:5]

    # Departamentos
    deptos_cnt = defaultdict(int)
    for a in atends:
        deptos_cnt[DEPTOS.get(a.get('setor',''),'?')] += 1
    deptos_top = sorted(deptos_cnt.items(), key=lambda x: x[1], reverse=True)[:4]

    # Pico de hora
    horas = defaultdict(int)
    for a in atends:
        try:
            h = (datetime.fromisoformat(a['date'].replace('Z','+00:00')).astimezone(BRT)).hour
            horas[h] += 1
        except: pass
    pico_h, pico_v = max(horas.items(), key=lambda x: x[1]) if horas else (0,0)

    # Alertas do dia
    alertas = []
    if len(sem) > 0:
        alertas.append(f'🚨 {len(sem)} cliente(s) ficaram sem atendente')
    if t_medio > 120:
        alertas.append(f'⚠️ Tempo médio crítico: {t_medio} min')
    if len(ea) > 0:
        alertas.append(f'⚠️ {len(ea)} atendimento(s) ficaram pendentes')
    longos = sum(1 for a in fin if any(
        (datetime.fromisoformat(a.get('fim','').replace('Z','+00:00')) -
         datetime.fromisoformat(a.get('date','').replace('Z','+00:00'))).total_seconds()/60 > 120
        for _ in [1] if a.get('fim') and a.get('date')
    ))
    if longos > 5:
        alertas.append(f'⚠️ {longos} atendimentos duraram mais de 2h')

    # Montar emoji de qualidade
    if pct >= 85 and t_medio <= 60:
        qualidade = '🟢 Ótimo'
    elif pct >= 70 and t_medio <= 120:
        qualidade = '🟡 Regular'
    else:
        qualidade = '🔴 Crítico'

    # Ranking texto
    medals = ['🥇','🥈','🥉','4️⃣','5️⃣']
    rank_txt = '\n'.join(
        f'{medals[i]} {NOMES.get(id, id[:8])} — {v["total"]} atend. | {round(v["fin"]/v["total"]*100)}% finaliz.'
        for i,(id,v) in enumerate(rank_sorted)
    )

    # Motivos texto
    motivos_txt = '\n'.join(
        f'  • {MOTIVOS.get(id, id[:10])}: {cnt}'
        for id,cnt in motivos_top
    )

    # Deptos texto
    deptos_txt = ' | '.join(f'{d}: {c}' for d,c in deptos_top)

    # Alertas texto
    alertas_txt = '\n'.join(alertas) if alertas else '✅ Nenhum alerta crítico'

    msg = (
        f'📊 *RELATÓRIO DE ATENDIMENTOS*\n'
        f'_{dia_sem}, {dia_fmt}_ — Clique DF Telecom\n'
        f'Qualidade do dia: {qualidade}\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📈 *RESUMO GERAL*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📬 Total: *{total}* atendimentos\n'
        f'✅ Finalizados: *{len(fin)} ({pct}%)*\n'
        f'⏳ Pendentes: *{len(ea)}*\n'
        f'🚫 Sem atendente: *{len(sem)}*\n'
        f'⏱️ Tempo médio: *{t_medio} min*\n'
        f'⏱️ Maior atendimento: *{t_max} min*\n'
        f'🔥 Pico: *{pico_h}h* com {pico_v} chamados\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'🏆 *RANKING*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'{rank_txt}\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'💬 *PRINCIPAIS MOTIVOS*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'{motivos_txt}\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📂 *DEPARTAMENTOS*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'{deptos_txt}\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'🚨 *ALERTAS DO DIA*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'{alertas_txt}\n\n'
        f'_Gerado automaticamente — Opa Suite_'
    )

    await telegram(msg)
    print(f'[{agora.strftime("%H:%M")}] Relatório de {dia_fmt} enviado!')


if __name__ == '__main__':
    asyncio.run(main())
