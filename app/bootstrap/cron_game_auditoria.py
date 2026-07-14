"""
Cron GAME Auditoria — roda todo dia às 20h (0 20 * * 1-6)
e todo domingo às 8h para auditoria do mês (0 8 * * 0)
Compara Opa vs banco e recalcula se houver divergência.
"""
import httpx, json, asyncio, sqlite3, os
from datetime import datetime, timezone, timedelta, date

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))
DB_PATH   = os.path.join(os.path.dirname(__file__), '../../hub_comercial.db')

ATENDENTES = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '67602d9691afc2bf7a36ed6c': 'Leide Aquino',
    '66d9f04910150407b4f311f9': 'Bruna Mathias',
}

DEPTO_SUPORTE    = '5bf73d1d186f7d2b0d647a61'
DEPTO_FINANCEIRO = '5d1624085e74a002308aa25e'

XP_MOTIVOS = {
    '65a18e3bae4972531a90d0a1':30,'6643c64684d5f75ec0a9155a':15,
    '6643c622bd1e771abfc338d2':20,'65a18da2ae4972531a90d014':20,
    '65a18dd2ae4972531a90d030':15,'65a18e11ae4972531a90d06d':15,
    '65a18d45f2a21eee31c88395':15,'65a18d55ae4972531a90cfd3':20,
    '65a18e04f2a21eee31c8843a':20,'65a18de1ae4972531a90d03d':10,
    '65a18d38ae4972531a90cfae':10,'65a18d77ae4972531a90d001':10,
    '65a18e4ef2a21eee31c88491':10,'65a18dbbf2a21eee31c883fe':10,
    '65a18e2ef2a21eee31c8846e':5,'665a205084d5f75ec0b077de':5,
    '65a18d64f2a21eee31c883cc':10,'6643c622bd1e771abfc338d2':20,
}
MOTIVOS_SUPORTE    = {'6643c64684d5f75ec0a9155a','6643c622bd1e771abfc338d2','65a18da2ae4972531a90d014','65a18dd2ae4972531a90d030'}
MOTIVOS_FINANCEIRO = {'65a18e11ae4972531a90d06d','65a18d45f2a21eee31c88395','65a18d55ae4972531a90cfd3','65a18e04f2a21eee31c8843a'}

def calc_xp(motivos, setor):
    eh_s = setor == DEPTO_SUPORTE
    eh_f = setor == DEPTO_FINANCEIRO
    melhor = 0
    for m in motivos:
        mid = m.get('idMotivo')
        _id = mid.get('_id','') if isinstance(mid,dict) else str(mid or '')
        if _id not in XP_MOTIVOS: continue
        xp_m = XP_MOTIVOS[_id]
        if _id == '65a18e3bae4972531a90d0a1':
            xp_m = 30 if eh_s else (15 if eh_f else 10)
        elif _id in MOTIVOS_SUPORTE and not eh_s: xp_m = 5
        elif _id in MOTIVOS_FINANCEIRO and not eh_f: xp_m = 5
        if xp_m > melhor: melhor = xp_m
    return 5 + melhor

def db_conn():
    conn = sqlite3.connect(os.path.abspath(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

async def telegram(msg):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})

async def buscar_dia(day: str) -> list:
    payload = {'filter':{'dataInicialAbertura':day,'dataFinalAbertura':day,'status':'F'},'options':{'limit':500}}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.request('GET', f'{OPA_BASE}/atendimento',
            headers={'Authorization':f'Bearer {OPA_TOKEN}','Content-Type':'application/json'},
            content=json.dumps(payload).encode())
    return r.json().get('data',[])

async def recalcular_mes(mes_inicio: int, mes_fim: int, ano: int = 2026):
    """Recalcula XP do mês completo buscando dia a dia no Opa"""
    from collections import defaultdict
    stats = defaultdict(lambda:{'xp':0,'atend':0,'xp_hoje':0,'atend_hoje':0})
    protos = set()
    hoje_str = str(datetime.now(BRT).date())

    for day_n in range(mes_inicio, mes_fim+1):
        day = f'{ano}-07-{day_n:02d}'
        atends = await buscar_dia(day)
        for a in atends:
            proto = a.get('protocolo','')
            if proto in protos: continue
            protos.add(proto)
            id_atd = a.get('id_atendente','')
            if isinstance(id_atd,dict): id_atd=id_atd.get('_id','')
            if id_atd not in ATENDENTES: continue
            xp = calc_xp(a.get('motivos',[]), a.get('setor',''))
            stats[id_atd]['xp'] += xp
            stats[id_atd]['atend'] += 1
            if day == hoje_str:
                stats[id_atd]['xp_hoje'] += xp
                stats[id_atd]['atend_hoje'] += 1

    agora = datetime.now(BRT).strftime('%Y-%m-%d %H:%M:%S')
    from app.bootstrap.cron_game_xp import get_nivel
    conn = db_conn()
    for atd_id, v in stats.items():
        nome = ATENDENTES[atd_id]
        nivel = get_nivel(v['xp'])[1]
        conn.execute('''INSERT OR REPLACE INTO game_atendentes
            (id,nome,nivel,xp_total,xp_mes,xp_hoje,atendimentos_total,atendimentos_hoje,data_ultimo_calculo,criado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (atd_id,nome,nivel,v['xp'],v['xp'],v['xp_hoje'],v['atend'],v['atend_hoje'],hoje_str,agora))
        conn.execute('''INSERT OR REPLACE INTO game_historico_mensal
            (atendente_id,nome,mes,atendimentos,xp,score)
            VALUES (?,?,?,?,?,?)''',
            (atd_id,nome,'2026-07',v['atend'],v['xp'],v['xp']+min(v['atend'],300)*2))
    conn.commit()
    conn.close()
    return stats

async def main():
    agora = datetime.now(BRT)
    hoje  = agora.date()
    hoje_str = str(hoje)
    is_domingo = agora.weekday() == 6

    print(f'[{agora.strftime("%H:%M")}] Auditoria GAME XP — {hoje_str}')

    # 1. Verificar divergência do dia
    atends_opa = await buscar_dia(hoje_str)
    opa_count = {}
    protos = set()
    for a in atends_opa:
        proto = a.get('protocolo','')
        if proto in protos: continue
        protos.add(proto)
        id_atd = a.get('id_atendente','')
        if isinstance(id_atd,dict): id_atd=id_atd.get('_id','')
        if id_atd in ATENDENTES:
            opa_count[id_atd] = opa_count.get(id_atd,0) + 1

    conn = db_conn()
    banco = {r['id']: r['atendimentos_hoje'] for r in
             conn.execute('SELECT id, atendimentos_hoje FROM game_atendentes').fetchall()}
    conn.close()

    divergencias = []
    for atd_id, cnt_opa in opa_count.items():
        cnt_banco = banco.get(atd_id, 0)
        if abs(cnt_opa - cnt_banco) > 2:  # tolerância de 2
            divergencias.append(f'{ATENDENTES[atd_id]}: Opa={cnt_opa} Banco={cnt_banco}')

    if divergencias:
        print(f'Divergências encontradas: {len(divergencias)}')
        # Recalcular mês completo
        stats = await recalcular_mes(1, hoje.day)
        msg = (
            f'🔧 *GAME ISP — Auditoria automática*\n'
            f'Divergências encontradas e corrigidas:\n'
            + '\n'.join([f'• {d}' for d in divergencias])
        )
        await telegram(msg)
        print('Recalculado e notificado.')
    else:
        print('Sem divergências.')

    # 2. Auditoria completa do mês todo domingo
    if is_domingo:
        print('Domingo — auditoria completa do mês...')
        stats = await recalcular_mes(1, hoje.day)
        total_xp = sum(v['xp'] for v in stats.values())
        total_atend = sum(v['atend'] for v in stats.values())
        print(f'Auditoria completa: {total_atend} atend | {total_xp} XP')
        await telegram(
            f'📊 *GAME ISP — Auditoria Semanal*\n'
            f'Mês recalculado com sucesso.\n'
            f'Total: {total_atend} atendimentos | {total_xp} XP'
        )

if __name__ == '__main__':
    asyncio.run(main())
