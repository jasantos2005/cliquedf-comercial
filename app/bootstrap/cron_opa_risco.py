"""
Cron OPA Risco — roda todo dia às 19h30
Cruza atendimentos abertos no Opa com faturas vencidas no IXC
Identifica clientes em risco máximo de cancelamento
"""
import httpx, json, asyncio
from datetime import date, datetime, timezone, timedelta
from app.services.ixc_db import ixc_select

OPA_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1OWMzYjk5ZjJhMjFlZWUzMWM3YWEzYSIsImlhdCI6MTc3MDgzODM5OH0.VNIC3HqVGIxuHQoesd-5jftTVkEMd6jionH9pkyKeAM'
OPA_BASE  = 'https://cliquedf.opasuite.com.br/api/v1'
TG_TOKEN  = '8027006096:AAHiJEdtFyPresI81tWgs-Je2PKdaYAyWtY'
TG_CHAT   = '-5142280642'
BRT       = timezone(timedelta(hours=-3))

DEPTOS = {
    '5bf73d1d186f7d2b0d647a61': 'Suporte',
    '5bf73d1d186f7d2b0d647a60': 'Comercial',
    '5d1624085e74a002308aa25e': 'Financeiro',
    '5bf73d1d186f7d2b0d647a64': 'Ag. Virtual',
    '5d1629315e74a002308aa262': 'Renegociações',
}


async def telegram(msg: str):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={'chat_id': TG_CHAT, 'text': msg, 'parse_mode': 'Markdown'})


async def buscar_opa_abertos():
    """Busca atendimentos em andamento no Opa de hoje"""
    hoje = str(date.today())
    payload = {"filter": {"dataInicialAbertura": hoje, "dataFinalAbertura": hoje}, "options": {"limit": 500}}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.request(
            method='GET',
            url=f'{OPA_BASE}/atendimento',
            headers={'Authorization': f'Bearer {OPA_TOKEN}', 'Content-Type': 'application/json'},
            content=json.dumps(payload).encode()
        )
    atends = r.json().get('data', [])
    # Retorna apenas os que estão em andamento (EA)
    return [a for a in atends if a.get('status') == 'EA']


def formatar_tel_ixc(telefone: str) -> str:
    """Converte 5579998875639@c.us para (79) 99988-5639"""
    tel = telefone.replace('@c.us', '')
    # Remover prefixo 55 (Brasil)
    if tel.startswith('55') and len(tel) >= 12:
        tel = tel[2:]
    # Agora deve ter 10 ou 11 dígitos: DDD + número
    tel = ''.join(c for c in tel if c.isdigit())
    if len(tel) == 11:  # celular com 9
        return f'({tel[:2]}) {tel[2:7]}-{tel[7:]}'
    elif len(tel) == 10:  # fixo
        return f'({tel[:2]}) {tel[2:6]}-{tel[6:]}'
    return tel

def buscar_fatura_vencida(telefone: str):
    """Busca cliente no IXC pelo telefone e verifica faturas vencidas"""
    if not telefone:
        return None

    tel_fmt = formatar_tel_ixc(telefone)
    if not tel_fmt or len(tel_fmt) < 8:
        return None

    # Buscar pelos últimos 9 dígitos para flexibilidade
    tel_digits = ''.join(c for c in tel_fmt if c.isdigit())
    tel_suffix = tel_digits[-9:]

    sql = """
        SELECT 
            c.id as cliente_id,
            c.razao as nome,
            c.telefone_celular,
            c.whatsapp,
            COUNT(f.id) as faturas_vencidas,
            MAX(f.valor) as maior_fatura,
            MIN(f.data_vencimento) as vencimento_mais_antigo,
            SUM(f.valor) as total_devido
        FROM cliente c
        JOIN fn_areceber f ON f.id_cliente = c.id
        WHERE 
            (c.telefone_celular LIKE %s OR c.fone LIKE %s OR c.whatsapp LIKE %s)
            AND f.status = 'A'
            AND f.data_vencimento < CURDATE()
        GROUP BY c.id
        HAVING faturas_vencidas > 0
        LIMIT 1
    """
    try:
        return ixc_select(sql, (f'%{tel_suffix}', f'%{tel_suffix}', f'%{tel_suffix}'))
    except:
        return None


async def main():
    agora   = datetime.now(BRT)
    hoje_fmt = agora.strftime('%d/%m/%Y')

    print(f'[{agora.strftime("%H:%M")}] Buscando atendimentos abertos no Opa...')
    abertos = await buscar_opa_abertos()
    print(f'[{agora.strftime("%H:%M")}] {len(abertos)} atendimentos em andamento.')

    if not abertos:
        print('Nenhum atendimento aberto.')
        return

    # Cruzar com IXC
    em_risco = []
    for a in abertos:
        canal_cliente = a.get('canal_cliente', '')
        if not canal_cliente:
            continue

        resultado = buscar_fatura_vencida(canal_cliente)
        if not resultado:
            continue

        for r in resultado:
            depto   = DEPTOS.get(a.get('setor', ''), '?')
            mins    = int((agora - datetime.fromisoformat(a['date'].replace('Z','+00:00')).astimezone(BRT)).total_seconds() / 60)
            dias_venc = (date.today() - r['vencimento_mais_antigo']).days if r.get('vencimento_mais_antigo') else 0

            em_risco.append({
                'nome':         r['nome'],
                'protocolo':    a.get('protocolo', '?'),
                'depto':        depto,
                'mins_atend':   mins,
                'faturas':      r['faturas_vencidas'],
                'total_devido': float(r['total_devido'] or 0),
                'dias_venc':    dias_venc,
            })

    if not em_risco:
        await telegram(
            f'✅ *CRUZAMENTO OPA + IXC — {hoje_fmt}*\n'
            f'Nenhum cliente com atendimento aberto e fatura vencida simultaneamente.'
        )
        print('Nenhum cliente em risco encontrado.')
        return

    # Ordenar por maior risco (mais dias vencido + mais tempo no atendimento)
    em_risco.sort(key=lambda x: (x['dias_venc'] + x['mins_atend']//60), reverse=True)

    total_devido = sum(r['total_devido'] for r in em_risco)
    linhas = []
    for r in em_risco[:10]:
        icon = '🔴' if r['dias_venc'] > 30 or r['mins_atend'] > 120 else '🟡'
        linhas.append(
            f"{icon} *{r['nome'][:25]}*\n"
            f"   📋 {r['protocolo']} | {r['depto']} | {r['mins_atend']}min aberto\n"
            f"   💸 {r['faturas']} fatura(s) vencida(s) há {r['dias_venc']} dias | R$ {r['total_devido']:.2f}"
        )

    msg = (
        f'🚨 *CLIENTES EM RISCO — {hoje_fmt}*\n'
        f'_Atendimento aberto no Opa + Fatura vencida no IXC_\n\n'
        f'Total em risco: *{len(em_risco)} cliente(s)*\n'
        f'Total em débito: *R$ {total_devido:.2f}*\n\n'
        + '\n\n'.join(linhas)
        + (f'\n\n_...e mais {len(em_risco)-10} cliente(s)_' if len(em_risco) > 10 else '')
        + '\n\n_Gerado automaticamente — Opa + IXC_'
    )

    await telegram(msg)
    print(f'[{agora.strftime("%H:%M")}] {len(em_risco)} clientes em risco. Total: R$ {total_devido:.2f}')


if __name__ == '__main__':
    asyncio.run(main())
