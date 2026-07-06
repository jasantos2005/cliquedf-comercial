"""
Cron OS Pendente — roda todo dia às 20h (0 20 * * 1-6)
Verifica atendimentos Opa finalizados com motivos que sugerem OS
e cruza com IXC para ver se OS foi realmente aberta no mesmo dia.
Alerta no Telegram os casos onde OS deveria ter sido aberta mas não foi.
"""
import httpx, json, asyncio, os
from datetime import date, datetime, timezone, timedelta
from app.services.ixc_db import ixc_conn

CONTROLE = '/tmp/opa_os_pendente_disparados.json'

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))

NOMES_ATD = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '67602d9691afc2bf7a36ed6c': 'Leide Aquino',
    '66d9f04910150407b4f311f9': 'Bruna Mathias',
    '659c3b99f2a21eee31c7aa3a': 'Ailton Santos',
    '5d1642ad4b16a50312cc8f4d': 'Caique (bot)',
}

# Motivos que indicam que deveria ter OS
MOTIVOS_OS = {
    '65a18da2ae4972531a90d014': 'Dúvidas visita técnica',
    '6643c64684d5f75ec0a9155a': 'Verificar conexão',
    '6643c622bd1e771abfc338d2': 'Sem acesso',
    '65a18e2ef2a21eee31c8846e': 'Transferido outro setor',
}

def carregar_controle():
    hoje = str(date.today())
    if not os.path.exists(CONTROLE):
        return {'data': hoje, 'protocolos': []}
    with open(CONTROLE) as f:
        ctrl = json.load(f)
    if ctrl.get('data') != hoje:
        return {'data': hoje, 'protocolos': []}
    return ctrl


def salvar_controle(ctrl):
    with open(CONTROLE, 'w') as f:
        json.dump(ctrl, f)


def formatar_tel(tel: str) -> str:
    digits = ''.join(c for c in tel if c.isdigit())
    if digits.startswith('55') and len(digits) >= 12:
        digits = digits[2:]
    if len(digits) == 11:
        return f'({digits[:2]}) {digits[2:7]}-{digits[7:]}'
    elif len(digits) == 10:
        return f'({digits[:2]}) {digits[2:6]}-{digits[6:]}'
    return tel

def buscar_nome_cliente(tel_fmt: str) -> str:
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT razao FROM cliente WHERE telefone_celular=%s OR fone=%s OR whatsapp=%s LIMIT 1',
                (tel_fmt, tel_fmt, tel_fmt)
            )
            r = cur.fetchone()
            return r['razao'] if r else '?'
    except:
        return '?'

def verificar_os_aberta(tel_fmt: str, data: str) -> int:
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT COUNT(*) as total FROM su_oss_chamado o'
                ' JOIN cliente c ON c.id = o.id_cliente'
                ' WHERE DATE(o.data_abertura) = %s'
                ' AND (c.telefone_celular=%s OR c.fone=%s OR c.whatsapp=%s)',
                (data, tel_fmt, tel_fmt, tel_fmt)
            )
            return cur.fetchone()['total']
    except:
        return -1

async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})

async def main():
    agora = datetime.now(BRT)
    hoje  = str(agora.date())

    # Buscar atendimentos finalizados do dia
    payload = {'filter':{'dataInicialAbertura':hoje,'dataFinalAbertura':hoje,'status':'F'},'options':{'limit':500}}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.request('GET', f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}','Content-Type':'application/json'},
            content=json.dumps(payload).encode())
    atends = r.json().get('data',[])

    # Filtrar atendimentos com motivos que sugerem OS
    sem_os = []
    vistos = set()

    # Filtrar só departamento Suporte
    atends = [a for a in atends if a.get('setor') == '5bf73d1d186f7d2b0d647a61']
    print(f'Suporte finalizados: {len(atends)}')
    for a in atends:
        proto = a.get('protocolo','')
        if proto in vistos:
            continue

        for m in a.get('motivos',[]):
            mid = m.get('idMotivo')
            _id = mid.get('_id','') if isinstance(mid,dict) else str(mid or '')
            if _id not in MOTIVOS_OS:
                continue

            tel_opa = (a.get('canal_cliente') or '').replace('@c.us','')
            tel_fmt = formatar_tel(tel_opa)
            qtd_os  = verificar_os_aberta(tel_fmt, hoje)

            if qtd_os == 0:
                nome    = buscar_nome_cliente(tel_fmt)
                atd_id  = a.get('id_atendente','')
                if isinstance(atd_id, dict): atd_id = atd_id.get('_id','')
                atd_nome = NOMES_ATD.get(atd_id, '?')
                sem_os.append({
                    'protocolo': proto,
                    'motivo':    MOTIVOS_OS[_id],
                    'tel':       tel_opa,
                    'nome':      nome,
                    'atendente': atd_nome,
                })
                vistos.add(proto)
                break

    print(f'[{agora.strftime("%H:%M")}] {len(sem_os)} atendimentos sem OS no IXC')

    ctrl = carregar_controle()
    ja_notificados = set(ctrl['protocolos'])
    novos = [s for s in sem_os if s['protocolo'] not in ja_notificados]

    if not novos:
        print(f'[{agora.strftime("%H:%M")}] Nenhum caso novo (todos já notificados hoje).')
        return

    sem_os = novos

    # Montar mensagem
    linhas = '\n'.join([
        f"⚠️ *{s['protocolo']}*\n"
        f"   👤 {s['nome']}\n"
        f"   📱 {s['tel']}\n"
        f"   🎯 Motivo: {s['motivo']}\n"
        f"   👩‍💼 Atendente: {s['atendente']}"
        for s in sem_os[:10]
    ])

    msg = (
        f"🔴 *OS NÃO ABERTA NO IXC — {agora.strftime('%d/%m %H:%M')}*\n"
        f"_{len(sem_os)} atendimento(s) finalizados no Opa com motivo que sugere OS, mas sem OS no IXC hoje_\n\n"
        f"{linhas}"
    )
    await telegram(msg)

    ctrl['protocolos'] = list(ja_notificados | {s['protocolo'] for s in sem_os})
    salvar_controle(ctrl)

if __name__ == '__main__':
    asyncio.run(main())
