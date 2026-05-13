"""
Hub Comercial — cron_valida_contratos.py
==========================================
Roda a cada hora.
Valida os contratos inseridos nas ultimas 2 horas no IXC
e notifica Ailton se algum campo estiver incorreto.
"""
import sqlite3, logging, os, sys, requests
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")
import pymysql, pymysql.cursors

DB_PATH = BASE_DIR / "hub_comercial.db"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_AILTON = "2135602169"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def notificar(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_AILTON, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")


def ixc():
    return pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        database="ixcprovedor", cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )


def main():
    log.info("=== Validando contratos recentes ===")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Pegar contratos ativados nas ultimas 2 horas
    rows = conn.execute("""
        SELECT p.id, p.razao, p.ixc_contrato_id, p.ixc_cliente_id,
               p.ixc_vendedor_id, p.taxa_instalacao, p.dia_vencimento,
               v.nome as vendedor_nome
        FROM hc_precadastros p
        LEFT JOIN hc_vendedores v ON v.id = p.ixc_vendedor_id
        WHERE p.status = 'ativado'
        AND p.ixc_contrato_id IS NOT NULL
        AND p.atualizado_em >= datetime('now', '-2 hours', '-3 hours')
    """).fetchall()

    if not rows:
        log.info("Nenhum contrato novo nas ultimas 2 horas.")
        conn.close()
        return

    log.info(f"Contratos para validar: {len(rows)}")
    cx = ixc()
    cur = cx.cursor()
    problemas_total = []

    for p in rows:
        cid = p['ixc_contrato_id']
        problemas = []

        cur.execute("""
            SELECT cc.id, cc.assinatura_digital, cc.taxa_instalacao,
                   cc.desconto_fidelidade, cc.id_vendedor, cc.id_vendedor_ativ,
                   cc.condicao_pagamento_primeira_fat,
                   v1.nome as vendedor_nome, v2.nome as vendedor_ativ_nome,
                   os.mensagem as os_msg, os.id as os_id
            FROM cliente_contrato cc
            LEFT JOIN vendedor v1 ON v1.id = cc.id_vendedor
            LEFT JOIN vendedor v2 ON v2.id = cc.id_vendedor_ativ
            LEFT JOIN su_oss_chamado os ON os.id_contrato_kit = cc.id
            WHERE cc.id = %s
        """, (cid,))
        cc = cur.fetchone()

        if not cc:
            problemas.append("Contrato não encontrado no IXC")
        else:
            # 1. Assinatura digital
            if cc['assinatura_digital'] != 'S':
                problemas.append(f"assinatura_digital={cc['assinatura_digital']} (esperado: S)")

            # 2. Vendedor correto
            # id_vendedor contrato = funcionario_ixc_id (colaborador)
            # id_vendedor_ativ contrato = ixc_vendedor_id (vendedor)
            vend_ativ_esperado = p['ixc_vendedor_id']
            # Buscar funcionario_ixc_id do vendedor
            import sqlite3 as _sq
            _c = _sq.connect(str(__import__("pathlib").Path(__file__).resolve().parent.parent.parent / "hub_comercial.db"))
            _row = _c.execute("SELECT funcionario_ixc_id FROM hc_vendedores WHERE id=? LIMIT 1", (vend_ativ_esperado,)).fetchone()
            vend_resp_esperado = _row[0] if _row else vend_ativ_esperado
            _c.close()
            if cc['id_vendedor_ativ'] != vend_ativ_esperado:
                problemas.append(f"id_vendedor_ativ={cc['id_vendedor_ativ']} ({cc['vendedor_ativ_nome']}) esperado={vend_ativ_esperado} ({p['vendedor_nome']})")

            # 3. Desconto fidelidade deve ser igual a taxa_instalacao no IXC
            taxa_ixc = float(cc['taxa_instalacao'] or 0)
            desc_ixc = float(cc['desconto_fidelidade'] or 0)
            if taxa_ixc > 0 and abs(desc_ixc - taxa_ixc) > 0.01:
                problemas.append(f"desconto_fidelidade={desc_ixc} diferente da taxa={taxa_ixc}")

            # 5. Condicao pagamento = dia escolhido
            venc_hub = str(p['dia_vencimento'] or '')
            venc_ixc = str(cc['condicao_pagamento_primeira_fat'] or '')
            if venc_hub and venc_hub not in venc_ixc:
                problemas.append(f"condicao_pagamento={venc_ixc} (Hub dia={venc_hub})")

            # 6. Mensagem OS tem campos obrigatorios
            msg_os = cc['os_msg'] or ''
            for campo in ['VIABILIDADE', 'PRAZOS', 'FIDELIDADE', 'COMODATO', 'ATIVAÇÃO FIBRA', 'PLANO']:
                if campo not in msg_os:
                    problemas.append(f"OS sem campo: {campo}")

        if problemas:
            bloco = f"*Contrato #{cid} — {p['razao']}*\n"
            for prob in problemas:
                bloco += f"  ⚠️ {prob}\n"
            problemas_total.append(bloco)
            log.warning(f"Contrato {cid} com {len(problemas)} problema(s)")
        else:
            log.info(f"Contrato {cid} OK")

    cx.close()
    conn.close()

    if problemas_total:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M")
        msg = f"🚨 *Validação de Contratos — {agora}*\n\n"
        msg += f"{len(problemas_total)} contrato(s) com problemas:\n\n"
        msg += "\n".join(problemas_total)
        notificar(msg)
        log.info(f"Notificação enviada: {len(problemas_total)} problemas")
    else:
        log.info(f"Todos os {len(rows)} contratos validados OK")


if __name__ == "__main__":
    main()
