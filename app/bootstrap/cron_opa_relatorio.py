"""
Cron OPA Relatório Diário — roda todo dia às 7h
Envia resumo do dia anterior no Telegram GESTÃO | COMERCIAL
"""
import httpx, json, asyncio, os
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

HISTORICO_ALERTAS = '/tmp/opa_alertas_historico.json'


def buscar_alertas_do_dia(data_str: str):
    if not os.path.exists(HISTORICO_ALERTAS):
        return 0, 0
    try:
        with open(HISTORICO_ALERTAS) as f:
            hist = json.load(f)
        dia = hist.get(data_str, {})
        return dia.get('sem_atendente', 0), dia.get('longos', 0)
    except:
        return 0, 0

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


def calcular_metricas(atends):
    total = len(atends)
    if total == 0:
        return {'total': 0, 'pct': 0, 't_medio_suporte': 0, 't_medio_financeiro': 0}
    fin = [a for a in atends if a.get('status') == 'F']
    pct = round(len(fin) / total * 100)

    SETOR_SUPORTE = '5bf73d1d186f7d2b0d647a61'
    SETOR_FINANCEIRO = '5d1624085e74a002308aa25e'
    SETORES = {SETOR_SUPORTE, SETOR_FINANCEIRO}
    tempos_suporte, tempos_financeiro = [], []
    for a in fin:
        setor = a.get('setor')
        if setor not in SETORES:
            continue
        try:
            m = (datetime.fromisoformat(a['fim'].replace('Z','+00:00')) -
                 datetime.fromisoformat(a['date'].replace('Z','+00:00'))).total_seconds() / 60
        except:
            continue
        if m > 0:
            (tempos_suporte if setor == SETOR_SUPORTE else tempos_financeiro).append(m)

    return {
        'total': total,
        'pct': pct,
        't_medio_suporte': round(sum(tempos_suporte)/len(tempos_suporte)) if tempos_suporte else 0,
        't_medio_financeiro': round(sum(tempos_financeiro)/len(tempos_financeiro)) if tempos_financeiro else 0,
    }


def seta(atual, anterior):
    if anterior == 0:
        return ''
    diff = atual - anterior
    if diff == 0:
        return ' (=)'
    pct_diff = round(abs(diff) / anterior * 100)
    icone = '🔺' if diff > 0 else '🔻'
    return f' ({icone}{pct_diff}%)'


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

    # Comparativo com o mesmo dia da semana passada (ex: sexta com sexta)
    semana_passada = ontem - timedelta(days=7)
    data_str_sem = semana_passada.strftime('%Y-%m-%d')
    dia_fmt_sem  = semana_passada.strftime('%d/%m')
    atends_sem_passada = await buscar(data_str_sem)
    metr_sem_passada = calcular_metricas(atends_sem_passada)

    fin  = [a for a in atends if a.get('status') == 'F']
    ea   = [a for a in atends if a.get('status') == 'EA']
    sem  = [a for a in atends if not a.get('id_atendente')]
    pct  = round(len(fin) / total * 100)

    # Tempo médio — considera apenas Suporte e Financeiro (Comercial/Renegociação
    # dependem do cliente enviar documentação/decidir, o que distorce o indicador)
    SETOR_SUPORTE = '5bf73d1d186f7d2b0d647a61'
    SETOR_FINANCEIRO = '5d1624085e74a002308aa25e'
    SETORES_TEMPO_MEDIO = {SETOR_SUPORTE, SETOR_FINANCEIRO}
    # Thresholds provisórios por setor — recalibrar após alguns dias de dado real
    LIMITE_SUPORTE = 40
    LIMITE_FINANCEIRO = 25
    tempos = []
    tempos_suporte = []
    tempos_financeiro = []
    for a in fin:
        setor = a.get('setor')
        if setor not in SETORES_TEMPO_MEDIO:
            continue
        try:
            m = (datetime.fromisoformat(a['fim'].replace('Z','+00:00')) -
                 datetime.fromisoformat(a['date'].replace('Z','+00:00'))).total_seconds() / 60
        except:
            continue
        if m > 0:
            tempos.append(m)
            (tempos_suporte if setor == SETOR_SUPORTE else tempos_financeiro).append(m)
    t_medio = round(sum(tempos)/len(tempos)) if tempos else 0
    t_max   = round(max(tempos)) if tempos else 0
    t_medio_suporte = round(sum(tempos_suporte)/len(tempos_suporte)) if tempos_suporte else 0
    t_medio_financeiro = round(sum(tempos_financeiro)/len(tempos_financeiro)) if tempos_financeiro else 0

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

    # Janela mais pesada — 3 horas consecutivas com maior soma de chamados
    janela_ini, janela_soma = 0, 0
    if horas:
        for h_ini in range(0, 22):
            soma = horas.get(h_ini,0) + horas.get(h_ini+1,0) + horas.get(h_ini+2,0)
            if soma > janela_soma:
                janela_ini, janela_soma = h_ini, soma

    alertas_sem_atd_dia, alertas_longos_dia = buscar_alertas_do_dia(data_str)

    # Alertas do dia
    alertas = []
    if len(sem) > 0:
        alertas.append(f'🚨 {len(sem)} cliente(s) ficaram sem atendente')
    if t_medio > 120:
        alertas.append(f'⚠️ Tempo médio crítico (Suporte/Financeiro): {t_medio} min')
    if t_medio_suporte > LIMITE_SUPORTE:
        alertas.append(f'⚠️ Tempo médio Suporte crítico: {t_medio_suporte} min (limite {LIMITE_SUPORTE} min)')
    if t_medio_financeiro > LIMITE_FINANCEIRO:
        alertas.append(f'⚠️ Tempo médio Financeiro crítico: {t_medio_financeiro} min (limite {LIMITE_FINANCEIRO} min)')
    if len(ea) > 0:
        alertas.append(f'⚠️ {len(ea)} atendimento(s) ficaram pendentes')
    longos = sum(1 for a in fin if a.get('setor') in SETORES_TEMPO_MEDIO and any(
        (datetime.fromisoformat(a.get('fim','').replace('Z','+00:00')) -
         datetime.fromisoformat(a.get('date','').replace('Z','+00:00'))).total_seconds()/60 > 120
        for _ in [1] if a.get('fim') and a.get('date')
    ))
    if longos > 5:
        alertas.append(f'⚠️ {longos} atendimentos duraram mais de 2h')
    if alertas_sem_atd_dia or alertas_longos_dia:
        alertas.append(
            f'🔔 Alertas disparados no dia: {alertas_sem_atd_dia} sem atendente | '
            f'{alertas_longos_dia} atendimento(s) longos'
        )

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
        f'Qualidade do dia: {qualidade}\n'
        f'✅ *Taxa de resolução: {pct}%*\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📈 *RESUMO GERAL*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📬 Total: *{total}* atendimentos\n'
        f'✅ Finalizados: *{len(fin)} ({pct}%)*\n'
        f'⏳ Pendentes: *{len(ea)}*\n'
        f'🚫 Sem atendente: *{len(sem)}*\n'
        f'⏱️ Tempo médio Suporte: *{t_medio_suporte} min*\n'
        f'⏱️ Tempo médio Financeiro: *{t_medio_financeiro} min*\n'
        f'⏱️ Maior atendimento (Suporte/Financeiro): *{t_max} min*\n'
        f'🔥 Pico: *{pico_h}h* com {pico_v} chamados\n'
        f'📶 Janela mais pesada: *{janela_ini}h-{janela_ini+3}h* ({janela_soma} chamados)\n\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📅 *COMPARATIVO — {dia_sem} passada ({dia_fmt_sem})*\n'
        f'━━━━━━━━━━━━━━━━━━━\n'
        f'📬 Total: {metr_sem_passada["total"]}{seta(total, metr_sem_passada["total"])}\n'
        f'✅ Resolução: {metr_sem_passada["pct"]}%{seta(pct, metr_sem_passada["pct"])}\n'
        f'⏱️ T.médio Suporte: {metr_sem_passada["t_medio_suporte"]}min{seta(t_medio_suporte, metr_sem_passada["t_medio_suporte"])}\n'
        f'⏱️ T.médio Financeiro: {metr_sem_passada["t_medio_financeiro"]}min{seta(t_medio_financeiro, metr_sem_passada["t_medio_financeiro"])}\n\n'
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
