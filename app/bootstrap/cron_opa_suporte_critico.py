"""
Cron OPA Suporte Crítico — roda a cada 30 minutos (*/30 7-22 * * *)
Dispara alerta no Telegram para atendimentos de SUPORTE parados há mais de 1h
Informa: cliente, protocolo, atendente responsável, tempo parado
"""
import httpx, json, asyncio
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8027006096:AAHiJEdtFyPresI81tWgs-Je2PKdaYAyWtY'
TG_CHAT   = '-5142280642'
BRT       = timezone(timedelta(hours=-3))
CONTROLE  = '/tmp/opa_suporte_critico.json'

NOMES = {
    '659c3d7dae4972531a907916': 'Johnatan David',
    '68c81c2e21ad7f45d635901f': 'Amanda Gomes',
    '682b6d07f497f37f8eb35338': 'Karine Ferreira',
    '6659e00cbd1e771abfd2aefc': 'Rudinedja Santos',
    '659c4448f2a21eee31c7ad36': 'Manuela Tavares',
    '5d1642ad4b16a50312cc8f4d': 'Caique (bot)',
}

DEPTO_SUPORTE = '5bf73d1d186f7d2b0d647a61'  # ID do departamento Suporte

LIMITE_CRITICO  = 60   # min — alerta crítico
LIMITE_GRAVE    = 120  # min — alerta gravíssimo


def carregar_controle():
    import os
    hoje = str(date.today())
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


async def buscar_atendimentos():
    hoje = str(date.today())
    payload = {"filter": {"dataInicialAbertura": hoje, "dataFinalAbertura": hoje}, "options": {"limit": 300}}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode()
        )
    return r.json().get('data', [])


async def buscar_detalhe(atend_id: str) -> dict:
    """Busca detalhe do atendimento com nome do cliente populado"""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f'{OPA_BASE}/atendimento/{atend_id}',
                headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'}
            )
        return r.json().get('data', {})
    except:
        return {}


async def main():
    agora = datetime.now(BRT)
    if agora.hour < 7 or agora.hour > 22:
        return

    atends = await buscar_atendimentos()
    ctrl   = carregar_controle()
    ja_alertados = set(ctrl['ids'])

    # Remover resolvidos do controle
    abertos = {a['_id'] for a in atends if a.get('status') in ('EA', 'AG')}
    resolvidos = ja_alertados - abertos
    if resolvidos:
        ctrl['ids'] = [i for i in ctrl['ids'] if i not in resolvidos]
        print(f'[{agora.strftime("%H:%M")}] {len(resolvidos)} resolvido(s) removido(s).')

    criticos  = []  # 60-120 min
    gravissimos = []  # >120 min

    for a in atends:
        _id    = a['_id']
        status = a.get('status')

        # Apenas suporte em andamento
        if status not in ('EA', 'AG'):
            continue
        if a.get('setor') != DEPTO_SUPORTE:
            continue
        if _id in ja_alertados:
            continue

        try:
            inicio = datetime.fromisoformat(a['date'].replace('Z', '+00:00')).astimezone(BRT)
            mins   = int((agora - inicio).total_seconds() / 60)
        except:
            continue

        atendente = NOMES.get(a.get('id_atendente', ''), None)
        proto     = a.get('protocolo', '?')
        canal_cli = a.get('canal_cliente', '').replace('@c.us', '')

        # Buscar nome do cliente e situação real no detalhe
        detalhe = await buscar_detalhe(_id)
        cliente_nome = '?'
        if isinstance(detalhe.get('id_cliente'), dict):
            cliente_nome = detalhe['id_cliente'].get('nome', '?')
        elif isinstance(detalhe.get('id_user'), dict):
            cliente_nome = detalhe['id_user'].get('nome', '?')

        # Determinar situação real
        tem_motivo  = len(detalhe.get('motivos', [])) > 0
        tem_obs     = len(detalhe.get('observacoes', [])) > 0
        if not a.get('id_atendente'):
            situacao = '❌ SEM ATENDENTE — ninguém pegou!'
        elif tem_motivo:
            situacao = '⏳ Motivo registrado — aguardando cliente'
        elif tem_obs:
            situacao = '⏳ Observação registrada — em acompanhamento'
        elif mins <= 30:
            situacao = '🟢 Atendimento recente — provavelmente em conversa'
        else:
            situacao = '⚠️ Sem registro interno — verificar conversa no Opa'

        info = {
            '_id':       _id,
            'proto':     proto,
            'mins':      mins,
            'atendente': atendente,
            'canal':     canal_cli,
            'status':    status,
            'cliente':   cliente_nome,
            'situacao':  situacao,
        }

        if mins >= LIMITE_GRAVE:
            gravissimos.append(info)
        elif mins >= LIMITE_CRITICO:
            criticos.append(info)

    # Ordenar por mais tempo parado
    gravissimos.sort(key=lambda x: x['mins'], reverse=True)
    criticos.sort(key=lambda x: x['mins'], reverse=True)

    novos_ids = []

    # Alerta gravíssimo
    if gravissimos:
        linhas = []
        for r in gravissimos[:8]:
            h   = r['mins'] // 60
            m   = r['mins'] % 60
            atd = r['atendente'] or '❌ SEM ATENDENTE'
            st  = '🟡 Em andamento' if r['status'] == 'EA' else '🔴 Aguardando'
            linhas.append(
                f"🔴 *{r['proto']}* — *{h}h{m:02d}min* parado\n"
                f"   🧑 Cliente: *{r['cliente'][:30]}*\n"
                f"   👤 Atendente: *{atd}*\n"
                f"   📋 {r['situacao']}\n"
                f"   📱 {r['canal']}"
            )
            novos_ids.append(r['_id'])

        msg = (
            f"🚨🚨 *SUPORTE GRAVÍSSIMO — {agora.strftime('%H:%M')}* 🚨🚨\n"
            f"_{len(gravissimos)} atendimento(s) de Suporte parado(s) há mais de 2h!_\n"
            f"_Cliente pode estar sem internet há horas — risco ALTO de cancelamento!_\n\n"
            + '\n\n'.join(linhas)
            + '\n\n_🔴 AÇÃO IMEDIATA NECESSÁRIA_'
        )
        await telegram(msg)
        await asyncio.sleep(1)

    # Alerta crítico
    if criticos:
        linhas = []
        for r in criticos[:8]:
            h   = r['mins'] // 60
            m   = r['mins'] % 60
            atd = r['atendente'] or '❌ SEM ATENDENTE'
            st  = '🟡 Em andamento' if r['status'] == 'EA' else '🔴 Aguardando'
            linhas.append(
                f"⚠️ *{r['proto']}* — *{h}h{m:02d}min* parado\n"
                f"   🧑 Cliente: *{r['cliente'][:30]}*\n"
                f"   👤 Atendente: *{atd}*\n"
                f"   📋 {r['situacao']}\n"
                f"   📱 {r['canal']}"
            )
            novos_ids.append(r['_id'])

        msg = (
            f"🚨 *SUPORTE CRÍTICO — {agora.strftime('%H:%M')}*\n"
            f"_{len(criticos)} atendimento(s) de Suporte parado(s) entre 1h e 2h_\n\n"
            + '\n\n'.join(linhas)
        )
        await telegram(msg)

    # Salvar controle
    for _id in novos_ids:
        ctrl['ids'].append(_id)
    salvar_controle(ctrl)

    total = len(gravissimos) + len(criticos)
    print(f'[{agora.strftime("%H:%M")}] {len(gravissimos)} gravíssimos | {len(criticos)} críticos | {total} alertas novos.')


if __name__ == '__main__':
    asyncio.run(main())
