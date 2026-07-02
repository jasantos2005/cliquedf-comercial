"""
Cron OPA OS — roda a cada hora (0 7-22 * * *)
Cruza OS abertas de suporte no IXC com atendimentos no Opa
Identifica clientes que abriram OS E estão no WhatsApp simultaneamente
"""
import httpx, json, asyncio
from datetime import date, datetime, timezone, timedelta
from app.services.ixc_db import ixc_select

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8308787747:AAFuP5Dr7wkOdbTvQhYI9BE5mQuDVDPgDIY'
TG_CHAT   = '2135602169'
BRT       = timezone(timedelta(hours=-3))

# Assuntos de suporte no IXC
ASSUNTOS_SUPORTE = {
    5:   'Suporte geral',
    17:  'Suporte técnico',
    20:  'Sem acesso',
    21:  'Internet lenta',
    27:  'Lentidão',
    44:  'Suporte WiFi',
    47:  'Configuração',
    94:  'Suporte equip.',
    102: 'Sem sinal',
    103: 'Instabilidade',
    104: 'Perda de pacote',
    105: 'Suporte roteador',
    107: 'Reinicialização',
    113: 'Suporte ONT',
    184: 'Suporte externo',
    203: 'Suporte interno',
    226: 'Suporte fibra',
    240: 'Manutenção',
    245: 'Suporte geral 2',
    248: 'Suporte técnico 2',
}

STATUS_OS = {
    'AG': 'Aguardando',
    'EN': 'Encaminhada',
    'AS': 'Assumida',
    'EX': 'Em execução',
    'RE': 'Reaberta',
}


async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})


def ixc_para_opa(tel_ixc: str) -> str:
    """Converte (79) 99959-8467 para 5579999598467@c.us"""
    digits = ''.join(c for c in tel_ixc if c.isdigit())
    return '55' + digits + '@c.us'


def buscar_os_abertas():
    """Busca OS de suporte abertas nos últimos 3 dias"""
    ids = ','.join(str(i) for i in ASSUNTOS_SUPORTE.keys())
    return ixc_select(f'''
        SELECT 
            o.id as os_id,
            o.id_assunto,
            o.status as os_status,
            o.data_abertura,
            c.id as cliente_id,
            c.razao as cliente,
            c.telefone_celular,
            c.whatsapp
        FROM su_oss_chamado o
        JOIN cliente c ON c.id = o.id_cliente
        WHERE o.id_assunto IN ({ids})
        AND o.status NOT IN ("F","C")
        AND DATE(o.data_abertura) >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
        ORDER BY o.data_abertura DESC
        LIMIT 200
    ''')


async def buscar_opa_hoje():
    """Busca todos atendimentos de hoje no Opa"""
    hoje = str(date.today())
    payload = {"filter": {"dataInicialAbertura": hoje, "dataFinalAbertura": hoje}, "options": {"limit": 500}}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode()
        )
    return r.json().get('data', [])


async def main():
    agora    = datetime.now(BRT)
    hoje_fmt = agora.strftime('%d/%m/%Y %H:%M')

    print(f'[{agora.strftime("%H:%M")}] Buscando OS abertas no IXC...')
    os_abertas = buscar_os_abertas()
    print(f'[{agora.strftime("%H:%M")}] {len(os_abertas)} OS de suporte abertas.')

    if not os_abertas:
        print('Nenhuma OS de suporte aberta.')
        return

    print(f'[{agora.strftime("%H:%M")}] Buscando atendimentos no Opa...')
    opa_atends = await buscar_opa_hoje()
    print(f'[{agora.strftime("%H:%M")}] {len(opa_atends)} atendimentos no Opa hoje.')

    # Mapear Opa por canal_cliente (telefone)
    opa_por_tel = {}
    for a in opa_atends:
        canal = a.get('canal_cliente', '')
        if canal:
            opa_por_tel[canal] = a

    # Cruzar OS com Opa
    cruzados    = []  # OS aberta + atendimento no Opa
    sem_opa     = []  # OS aberta mas SEM atendimento no Opa (cliente não entrou em contato)

    for os in os_abertas:
        tel_cel  = os.get('telefone_celular', '')
        tel_wpp  = os.get('whatsapp', '')
        opa_tel1 = ixc_para_opa(tel_cel) if tel_cel else ''
        opa_tel2 = ixc_para_opa(tel_wpp) if tel_wpp else ''

        atend_opa = opa_por_tel.get(opa_tel1) or opa_por_tel.get(opa_tel2)
        assunto   = ASSUNTOS_SUPORTE.get(os['id_assunto'], f"ID {os['id_assunto']}")
        status_os = STATUS_OS.get(os['os_status'], os['os_status'])
        horas_os  = round((agora - os['data_abertura'].replace(tzinfo=BRT)).total_seconds() / 3600, 1)

        if atend_opa:
            mins_opa = int((agora - datetime.fromisoformat(atend_opa['date'].replace('Z','+00:00')).astimezone(BRT)).total_seconds() / 60)
            cruzados.append({
                'cliente':   os['cliente'],
                'os_id':     os['os_id'],
                'assunto':   assunto,
                'status_os': status_os,
                'horas_os':  horas_os,
                'protocolo': atend_opa.get('protocolo','?'),
                'status_opa': atend_opa.get('status','?'),
                'mins_opa':  mins_opa,
            })
        else:
            sem_opa.append({
                'cliente':   os['cliente'],
                'os_id':     os['os_id'],
                'assunto':   assunto,
                'status_os': status_os,
                'horas_os':  horas_os,
            })

    # Montar mensagem
    partes = []

    if cruzados:
        linhas = []
        for r in cruzados[:8]:
            icon_opa = '🟢' if r['status_opa'] == 'F' else '🔴'
            linhas.append(
                f"👤 *{r['cliente'][:25]}*\n"
                f"   🔧 OS#{r['os_id']} — {r['assunto']} | {r['status_os']} | {r['horas_os']}h aberta\n"
                f"   {icon_opa} Opa: {r['protocolo']} | {r['mins_opa']}min | Status: {r['status_opa']}"
            )
        partes.append(
            f"🔄 *OS + OPA — CLIENTES EM DUPLO ATENDIMENTO*\n"
            f"{len(cruzados)} cliente(s) com OS aberta E no WhatsApp:\n\n"
            + '\n\n'.join(linhas)
        )

    if sem_opa:
        linhas = []
        for r in sem_opa[:8]:
            linhas.append(
                f"  • *{r['cliente'][:25]}* — OS#{r['os_id']} | {r['assunto']} | {r['status_os']} | {r['horas_os']}h"
            )
        partes.append(
            f"📋 *OS SEM CONTATO NO OPA*\n"
            f"{len(sem_opa)} cliente(s) com OS aberta mas SEM atendimento no WhatsApp:\n"
            + '\n'.join(linhas)
        )

    if not partes:
        print('Nenhum cruzamento encontrado.')
        return

    msg = (
        f"🔧 *CRUZAMENTO OS + OPA — {hoje_fmt}*\n\n"
        + '\n\n━━━━━━━━━━━━━━━━━━━\n\n'.join(partes)
        + '\n\n_Gerado automaticamente — IXC + Opa_'
    )

    await telegram(msg)
    print(f'[{agora.strftime("%H:%M")}] Cruzamento: {len(cruzados)} duplos | {len(sem_opa)} sem Opa.')


if __name__ == '__main__':
    asyncio.run(main())
