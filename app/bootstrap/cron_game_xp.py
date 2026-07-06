"""
Cron GAME XP — roda a cada 30 min (*/30 8-19 * * 1-6)
Calcula XP dos atendentes baseado nos atendimentos do Opa do dia.
"""
import httpx, json, asyncio, sqlite3, os
from datetime import date, datetime, timezone, timedelta

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))
DB_PATH   = os.path.join(os.path.dirname(__file__), '../../hub_comercial.db')

DEPTO_SUPORTE    = '5bf73d1d186f7d2b0d647a61'
DEPTO_FINANCEIRO = '5d1624085e74a002308aa25e'

# Atendentes participantes
ATENDENTES = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '67602d9691afc2bf7a36ed6c': 'Leide Aquino',
    '66d9f04910150407b4f311f9': 'Bruna Mathias',
}

NIVEIS = [
    (0,    1, '🌱 Aprendiz'),
    (500,  2, '🔧 Atendente'),
    (1500, 3, '🚀 Especialista'),
    (3000, 4, '⭐ Analista'),
    (6000, 5, '🏆 Master ISP'),
    (10000,6, '👑 Mentor'),
]

# XP por motivo
XP_MOTIVOS = {
    # Suporte
    '65a18e3bae4972531a90d0a1': ('Resolvido no atendimento', 30),
    '6643c64684d5f75ec0a9155a': ('Verificar conexão', 15),
    '6643c622bd1e771abfc338d2': ('Sem acesso', 20),
    '65a18da2ae4972531a90d014': ('Dúvidas visita técnica', 20),
    '65a18dd2ae4972531a90d030': ('Troca SSID/Senha WiFi', 15),
    # Financeiro
    '65a18e11ae4972531a90d06d': ('2ª via de boleto', 15),
    '65a18d45f2a21eee31c88395': ('Comprovante de pagamento', 15),
    '65a18d55ae4972531a90cfd3': ('Promessa de pagamento', 20),
    '65a18e04f2a21eee31c8843a': ('Habilitar/Desbloqueio', 20),
    # Comercial/Geral
    '65a18de1ae4972531a90d03d': ('Atualização cadastral', 10),
    '65a18d38ae4972531a90cfae': ('Central do cliente', 10),
    '65a18d77ae4972531a90d001': ('Dúvidas sobre contrato', 10),
    '65a18e4ef2a21eee31c88491': ('Orientação/Dúvida', 10),
    '65a18dbbf2a21eee31c883fe': ('Informação de sinistro', 10),
    '65a18e2ef2a21eee31c8846e': ('Transferido outro setor', 5),
    '665a205084d5f75ec0b077de': ('Falta de comunicação', 5),
    '65a18d64f2a21eee31c883cc': ('Mudança de endereço', 10),
}

def get_nivel(xp: int) -> tuple:
    nivel = NIVEIS[0]
    for xp_min, num, nome in NIVEIS:
        if xp >= xp_min:
            nivel = (xp_min, num, nome)
    return nivel

def db_conn():
    conn = sqlite3.connect(os.path.abspath(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})

async def buscar_atendimentos(hoje: str) -> list:
    payload = {'filter':{'dataInicialAbertura':hoje,'dataFinalAbertura':hoje,'status':'F'},'options':{'limit':500}}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.request('GET', f'{OPA_BASE}/atendimento',
            headers={'Authorization':f'Bearer {OPA_TOKEN}','Content-Type':'application/json'},
            content=json.dumps(payload).encode())
    return r.json().get('data',[])

async def main(fechar_dia: bool = False):
    agora = datetime.now(BRT)
    hoje  = str(agora.date())

    atends = await buscar_atendimentos(hoje)

    # Calcular XP por atendente
    xp_dia = {_id: {'xp': 0, 'atend': 0, 'eventos': []} for _id in ATENDENTES}

    # Protocolos já processados (evitar duplicar)
    protos_vistos = set()

    for a in atends:
        id_atd = a.get('id_atendente','')
        if isinstance(id_atd, dict): id_atd = id_atd.get('_id','')
        if id_atd not in ATENDENTES:
            continue

        proto = a.get('protocolo','')
        if proto in protos_vistos:
            continue
        protos_vistos.add(proto)

        xp_dia[id_atd]['atend'] += 1
        xp_base = 5  # XP base por atendimento finalizado

        # XP pelo motivo
        motivos = a.get('motivos',[])
        melhor_xp = 0
        melhor_motivo = 'Atendimento finalizado'
        for m in motivos:
            mid = m.get('idMotivo')
            _id = mid.get('_id','') if isinstance(mid,dict) else str(mid or '')
            if _id in XP_MOTIVOS:
                nome_m, xp_m = XP_MOTIVOS[_id]
                if xp_m > melhor_xp:
                    melhor_xp = xp_m
                    melhor_motivo = nome_m

        xp_total = xp_base + melhor_xp
        xp_dia[id_atd]['xp'] += xp_total
        xp_dia[id_atd]['eventos'].append({
            'protocolo': proto,
            'motivo': melhor_motivo,
            'xp': xp_total,
        })

    # Salvar no banco
    conn = db_conn()
    try:
        for atd_id, dados in xp_dia.items():
            if dados['atend'] == 0:
                continue

            nome = ATENDENTES[atd_id]
            xp_hoje = dados['xp']
            atend_hoje = dados['atend']

            # Buscar XP total acumulado
            row = conn.execute('SELECT * FROM game_atendentes WHERE id=?', (atd_id,)).fetchone()
            if row:
                # Atualizar XP do dia (sobrescreve)
                xp_anterior_hoje = row['xp_hoje'] if row['data_ultimo_calculo'] == hoje else 0
                xp_total_novo = row['xp_total'] - xp_anterior_hoje + xp_hoje
                # Resetar xp_mes se mudou o mês
                mes_atual = hoje[:7]
                mes_ultimo = (row['data_ultimo_calculo'] or '')[:7]
                if mes_atual != mes_ultimo:
                    xp_mes_novo = xp_hoje  # novo mês, começa do zero
                else:
                    xp_mes_novo = row['xp_mes'] - xp_anterior_hoje + xp_hoje
                nivel_anterior = row['nivel']
                conn.execute('''UPDATE game_atendentes SET
                    xp_total=?, xp_mes=?, xp_hoje=?,
                    atendimentos_hoje=?, atendimentos_total=?,
                    nivel=?, data_ultimo_calculo=?
                    WHERE id=?''', (
                    xp_total_novo, xp_mes_novo, xp_hoje,
                    atend_hoje, row['atendimentos_total'] - (row['atendimentos_hoje'] if row['data_ultimo_calculo']==hoje else 0) + atend_hoje,
                    get_nivel(xp_total_novo)[1], hoje, atd_id
                ))
                # Notificar subida de nível
                nivel_novo = get_nivel(xp_total_novo)[1]
                if nivel_novo > nivel_anterior and fechar_dia:
                    nome_nivel = get_nivel(xp_total_novo)[2]
                    await telegram(f'🎉 *SUBIU DE NÍVEL!*\n👤 *{nome}* alcançou *{nome_nivel}*!\n⭐ {xp_total_novo} XP total')
            else:
                nivel = get_nivel(xp_hoje)[1]
                conn.execute('''INSERT INTO game_atendentes
                    (id,nome,nivel,xp_total,xp_mes,xp_hoje,atendimentos_total,atendimentos_hoje,data_ultimo_calculo,criado_em)
                    VALUES (?,?,?,?,?,?,?,?,?,?)''',
                    (atd_id,nome,nivel,xp_hoje,xp_hoje,xp_hoje,atend_hoje,atend_hoje,hoje,agora.strftime('%Y-%m-%d %H:%M:%S')))

            # Salvar histórico (só no fechamento do dia)
            if fechar_dia:
                conn.execute('DELETE FROM game_xp_historico WHERE atendente_id=? AND data=?', (atd_id, hoje))
                for ev in dados['eventos']:
                    conn.execute('''INSERT INTO game_xp_historico
                        (atendente_id,protocolo,motivo,xp,descricao,data,criado_em)
                        VALUES (?,?,?,?,?,?,?)''',
                        (atd_id, ev['protocolo'], ev['motivo'], ev['xp'],
                         '+' + str(ev['xp']) + ' XP - ' + ev['motivo'], hoje,
                         agora.strftime('%Y-%m-%d %H:%M:%S')))

        conn.commit()
    finally:
        conn.close()

    # Relatório do dia (só no fechamento)
    if fechar_dia:
        conn = db_conn()
        ranking = conn.execute('''SELECT nome, xp_hoje, xp_total, nivel, atendimentos_hoje
            FROM game_atendentes WHERE data_ultimo_calculo=? AND xp_hoje > 0
            ORDER BY xp_hoje DESC''', (hoje,)).fetchall()
        conn.close()

        if ranking:
            medals = ['🥇','🥈','🥉','4º','5º','6º','7º']
            linhas = '\n'.join([
                f"{medals[i]} *{r['nome']}* +{r['xp_hoje']} XP | {r['atendimentos_hoje']} atend. | Total: {r['xp_total']} XP"
                for i, r in enumerate(ranking)
            ])
            msg = (
                f"🎮 *GAME ISP — {agora.strftime('%d/%m/%Y')}*\n"
                f"_Ranking de XP do dia_\n\n"
                f"{linhas}\n\n"
                f"_Próximo relatório amanhã às 19h_"
            )
            await telegram(msg)

    total_xp = sum(d['xp'] for d in xp_dia.values())
    print(f'[{agora.strftime("%H:%M")}] GAME XP calculado — {len([d for d in xp_dia.values() if d["atend"]>0])} atendentes | {total_xp} XP total')

if __name__ == '__main__':
    import sys
    fechar = '--fechar' in sys.argv
    asyncio.run(main(fechar_dia=fechar))
