"""
Cron Reincidência — roda a cada 2h (0 8-20/2 * * *)
Busca atendimentos abertos do dia, cruza com histórico de OS IXC
e envia alerta no Telegram com análise de risco de cancelamento
"""
import httpx, json, asyncio
from datetime import date, datetime, timezone, timedelta
from app.services.ixc_db import ixc_conn

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8027006096:AAHiJEdtFyPresI81tWgs-Je2PKdaYAyWtY'
TG_CHAT   = '-5142280642'
BRT       = timezone(timedelta(hours=-3))
CONTROLE  = '/tmp/opa_reincidencia.json'

NOMES_ATEND = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '5d1642ad4b16a50312cc8f4d': 'Caique (bot)',
}

def carregar_controle():
    import os
    hoje = str((datetime.now(BRT)).date())
    if not os.path.exists(CONTROLE):
        return {'data': hoje, 'ids': []}
    with open(CONTROLE) as f:
        ctrl = json.load(f)
    if ctrl.get('data') != hoje:
        return {'data': hoje, 'ids': []}
    return ctrl

def salvar_controle(ctrl):
    with open(CONTROLE, 'w') as f:
        json.dump(ctrl, f)

async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})

def formatar_tel(tel_opa: str) -> str:
    digits = ''.join(c for c in tel_opa if c.isdigit())
    if digits.startswith('55') and len(digits) >= 12:
        digits = digits[2:]
    if len(digits) == 11:
        return f'({digits[:2]}) {digits[2:7]}-{digits[7:]}'
    elif len(digits) == 10:
        return f'({digits[:2]}) {digits[2:6]}-{digits[6:]}'
    return tel_opa

def buscar_historico_cliente(tel_fmt: str) -> list:
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT o.id, o.id_assunto, o.status, o.data_abertura, o.data_fechamento,'
                ' o.mensagem as obs_abertura, f.funcionario as tecnico,'
                ' CASE o.id_assunto'
                ' WHEN 16 THEN \'Manutencao\''
                ' WHEN 20 THEN \'Sem acesso\''
                ' WHEN 21 THEN \'Internet lenta\''
                ' ELSE \'Outro\' END as assunto,'
                ' (SELECT m.mensagem FROM su_oss_chamado_mensagem m'
                '  WHERE m.id_chamado = o.id AND m.status = \'F\''
                '  ORDER BY m.data DESC LIMIT 1) as obs_fechamento'
                ' FROM su_oss_chamado o'
                ' JOIN cliente c ON c.id = o.id_cliente'
                ' LEFT JOIN funcionarios f ON f.id = o.id_tecnico'
                ' WHERE o.id_assunto IN (16, 20, 21)'
                ' AND (c.telefone_celular = %s OR c.fone = %s OR c.whatsapp = %s)'
                ' ORDER BY o.data_abertura DESC LIMIT 10',
                (tel_fmt, tel_fmt, tel_fmt)
            )
            return cur.fetchall()
    except:
        return []

def buscar_nome_cliente(tel_fmt: str) -> str:
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT razao FROM cliente'
                ' WHERE telefone_celular = %s OR fone = %s OR whatsapp = %s LIMIT 1',
                (tel_fmt, tel_fmt, tel_fmt)
            )
            row = cur.fetchone()
            return row['razao'] if row else '?'
    except:
        return '?'

def risco_cancelamento(qtd: int) -> str:
    if qtd >= 5:
        return '🔴 *RISCO CRÍTICO DE CANCELAMENTO* — mais de 5 ocorrências recentes!'
    elif qtd >= 3:
        return '🟠 *RISCO ALTO* — cliente com histórico recorrente de problemas'
    elif qtd >= 2:
        return '🟡 *RISCO MÉDIO* — 2ª ocorrência registrada'
    else:
        return '🟢 *Primeira reincidência* — monitorar'

async def main():
    agora = datetime.now(BRT)
    if agora.hour < 7 or agora.hour > 22:
        return

    hoje = str((datetime.now(BRT)).date())
    ctrl = carregar_controle()
    ja_alertados = set(ctrl['ids'])

    # Buscar atendimentos abertos do dia
    payload = {'filter': {'dataInicialAbertura': hoje, 'dataFinalAbertura': hoje}, 'options': {'limit': 200}}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.request('GET', f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode())
    atends = r.json().get('data', [])

    # Filtrar apenas abertos (EA ou AG)
    abertos = [a for a in atends if a.get('status') in ('EA', 'AG')]

    novos_ids = []
    alertas = []

    for a in abertos:
        _id = a['_id']
        if _id in ja_alertados:
            continue

        tel_opa = (a.get('canal_cliente') or '').replace('@c.us', '')
        if not tel_opa:
            continue

        tel_fmt = formatar_tel(tel_opa)
        historico = buscar_historico_cliente(tel_fmt)

        if not historico:
            continue

        nome = buscar_nome_cliente(tel_fmt)
        atendente = NOMES_ATEND.get(a.get('id_atendente', ''), 'Sem atendente')
        proto = a.get('protocolo', '?')
        qtd = len(historico)
        risco = risco_cancelamento(qtd)

        # Montar histórico formatado
        hist_txt = ''
        for os in historico[:3]:  # máximo 3 OS no Telegram
            dt = str(os['data_abertura'])[:10] if os['data_abertura'] else '?'
            obs_ab  = (os.get('obs_abertura') or '')[:150].replace('\n', ' ')
            obs_fec = (os.get('obs_fechamento') or '')[:150].replace('\n', ' ')
            hist_txt += (
                f"\n📌 *OS #{os['id']}* — {os['assunto']} ({dt})\n"
                f"   👷 Técnico: {os.get('tecnico','?')}\n"
            )
            if obs_ab:
                hist_txt += f"   📋 {obs_ab[:100]}\n"
            if obs_fec:
                hist_txt += f"   ✅ {obs_fec[:100]}\n"

        msg = (
            f"⚠️ *CLIENTE REINCIDENTE — {agora.strftime('%H:%M')}*\n"
            f"👤 *{nome}*\n"
            f"📱 {tel_opa}\n"
            f"🎫 Protocolo Opa: {proto}\n"
            f"👩‍💼 Atendente: {atendente}\n"
            f"📊 OS anteriores: *{qtd}* (assuntos 16/20/21)\n"
            f"{risco}\n"
            f"\n*Histórico recente:*{hist_txt}"
        )

        alertas.append(msg)
        novos_ids.append(_id)

    # Enviar alertas
    for msg in alertas[:5]:  # máximo 5 por vez
        await telegram(msg)
        await asyncio.sleep(1)

    # Salvar controle
    ctrl['ids'].extend(novos_ids)
    salvar_controle(ctrl)

    print(f'[{agora.strftime("%H:%M")}] {len(alertas)} alertas de reincidência enviados.')

if __name__ == '__main__':
    asyncio.run(main())
