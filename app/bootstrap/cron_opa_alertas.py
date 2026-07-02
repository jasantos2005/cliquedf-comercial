"""
Cron OPA Alertas — roda de hora em hora (0 7-22 * * *)
- Dispara alerta apenas UMA vez por atendimento
- Controle salvo em /tmp/opa_alertas_disparados.json
- Resumo horário enviado uma vez por hora
"""
import httpx, json, asyncio, os
from datetime import date, datetime, timezone, timedelta

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))
CONTROLE  = '/tmp/opa_alertas_disparados.json'

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

LIMITE_SEM_ATENDENTE = 15
LIMITE_EM_ANDAMENTO  = 60


def carregar_controle():
    hoje = str(date.today())
    if not os.path.exists(CONTROLE):
        return {'data': hoje, 'ids': [], 'ultimo_resumo': 0}
    with open(CONTROLE) as f:
        ctrl = json.load(f)
    if ctrl.get('data') != hoje:
        return {'data': hoje, 'ids': [], 'ultimo_resumo': 0}
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
    payload = {"filter": {"dataInicialAbertura": hoje, "dataFinalAbertura": hoje}, "options": {"limit": 200}}
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode()
        )
    return r.json().get('data', [])


async def main():
    agora = datetime.now(BRT)
    hora  = agora.hour
    if hora < 7 or hora > 22:
        return

    atends = await buscar_atendimentos()
    ctrl   = carregar_controle()
    ja_alertados = set(ctrl['ids'])

    # Remover do controle os que já foram resolvidos
    abertos    = {a['_id'] for a in atends if a.get('status') == 'EA'}
    resolvidos = ja_alertados - abertos
    if resolvidos:
        ctrl['ids'] = [i for i in ctrl['ids'] if i not in resolvidos]
        print(f"[{agora.strftime('%H:%M')}] {len(resolvidos)} resolvido(s) removido(s) do controle.")

    alertas_sem  = []
    alertas_long = []

    for a in atends:
        _id    = a['_id']
        status = a.get('status')
        if status != 'EA':
            continue
        if _id in ja_alertados:
            continue

        try:
            inicio = datetime.fromisoformat(a['date'].replace('Z', '+00:00')).astimezone(BRT)
            mins   = int((agora - inicio).total_seconds() / 60)
        except:
            continue

        depto = DEPTOS.get(a.get('setor', ''), '?')
        nome  = NOMES.get(a.get('id_atendente', ''), 'Sem atendente')
        proto = a.get('protocolo', '?')
        icon  = '🚨' if depto == 'Suporte' else '💰' if depto in ('Financeiro', 'Renegociações') else '🟡'

        if not a.get('id_atendente') and mins >= LIMITE_SEM_ATENDENTE:
            alertas_sem.append((f"  {icon} {proto} — {mins}min | {depto}", _id))
        elif a.get('id_atendente') and mins >= LIMITE_EM_ANDAMENTO:
            alertas_long.append((f"  {icon} {proto} — {mins}min | {depto} | {nome}", _id))

    # Alerta sem atendente
    if alertas_sem:
        linhas = '\n'.join(l for l, _ in alertas_sem[:5])
        await telegram(
            f"🚨 *CLIENTES SEM ATENDENTE* — {agora.strftime('%H:%M')}\n"
            f"{len(alertas_sem)} cliente(s) aguardando há mais de {LIMITE_SEM_ATENDENTE}min:\n"
            f"{linhas}"
        )
        for _, _id in alertas_sem:
            ctrl['ids'].append(_id)
        salvar_controle(ctrl)
        await asyncio.sleep(1)

    # Alerta atendimentos longos
    if alertas_long:
        linhas = '\n'.join(l for l, _ in alertas_long[:5])
        await telegram(
            f"⚠️ *ATENDIMENTOS LONGOS* — {agora.strftime('%H:%M')}\n"
            f"{len(alertas_long)} atendimento(s) aberto(s) há mais de {LIMITE_EM_ANDAMENTO}min:\n"
            f"{linhas}\n\n"
            f"_🚨 Suporte = cliente sem internet_\n"
            f"_💰 Financeiro/Renegociação = aguardando cliente_"
        )
        for _, _id in alertas_long:
            ctrl['ids'].append(_id)
        salvar_controle(ctrl)
        await asyncio.sleep(1)

    # Resumo horário — apenas 1x por hora
    agora_ts     = agora.timestamp()
    ultimo_resumo = ctrl.get('ultimo_resumo', 0)
    if (agora_ts - ultimo_resumo) >= 3300:
        total = len(atends)
        fin   = sum(1 for a in atends if a.get('status') == 'F')
        ea    = sum(1 for a in atends if a.get('status') == 'EA')
        sem   = sum(1 for a in atends if a.get('status') == 'EA' and not a.get('id_atendente'))
        pct   = round(fin / total * 100) if total else 0
        await telegram(
            f"📊 *RESUMO {agora.strftime('%H:%M')}*\n"
            f"Total: {total} | ✅ Finalizados: {fin} ({pct}%)\n"
            f"⏳ Em andamento: {ea} | 🚫 Sem atendente: {sem}"
        )
        ctrl['ultimo_resumo'] = agora_ts
        salvar_controle(ctrl)
    else:
        mins_prox = int((3300 - (agora_ts - ultimo_resumo)) / 60)
        print(f"[{agora.strftime('%H:%M')}] Resumo já enviado. Próximo em ~{mins_prox} min.")

    novos = len(alertas_sem) + len(alertas_long)
    print(f"[{agora.strftime('%H:%M')}] {novos} alerta(s) novo(s). Controle: {len(ctrl['ids'])} IDs.")


if __name__ == '__main__':
    asyncio.run(main())
