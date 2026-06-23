#!/usr/bin/env python3
"""
INSTALADOR — Retenção por Vencimento
Hub Comercial CliqueDF

Rodar UMA única vez:
  cd /opt/automacoes/cliquedf/comercial
  PYTHONPATH=. venv/bin/python3 /tmp/instalar_retencao.py
"""

import sys, os, sqlite3, shutil
sys.path.insert(0, ".")

BASE = "/opt/automacoes/cliquedf/comercial"

# ── 1. Cria tabela hc_retencao ────────────────────────────────────────────────
print("1️⃣  Criando tabela hc_retencao...")
conn = sqlite3.connect(f"{BASE}/hub_comercial.db")
conn.executescript("""
CREATE TABLE IF NOT EXISTS hc_retencao (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ixc_contrato_id     INTEGER NOT NULL UNIQUE,
    ixc_cliente_id      INTEGER,
    cliente             TEXT,
    plano_nome          TEXT,
    plano_valor         REAL,
    data_expiracao      TEXT,
    telefone            TEXT,
    cidade_nome         TEXT,
    status_retencao     TEXT NOT NULL DEFAULT 'pendente',
    obs                 TEXT,
    responsavel         TEXT,
    criado_em           TEXT DEFAULT (datetime('now','localtime')),
    atualizado_em       TEXT DEFAULT (datetime('now','localtime'))
);
""")
conn.commit()
conn.close()
print("   ✅ hc_retencao criada!\n")

# ── 2. Copia retencao.py para app/routes/ ─────────────────────────────────────
print("2️⃣  Copiando retencao.py para app/routes/...")
shutil.copy("/tmp/retencao_route.py", f"{BASE}/app/routes/retencao.py")
print("   ✅ app/routes/retencao.py instalado!\n")

# ── 3. Registra rota no main.py (se ainda não estiver) ───────────────────────
print("3️⃣  Registrando rota no main.py...")
main_path = f"{BASE}/app/main.py"
with open(main_path, "r") as f:
    main_content = f.read()

if "retencao_router" in main_content:
    print("   ⚠️  Rota já registrada, pulando.\n")
else:
    # Encontra último include_router e adiciona depois
    insert_import = "from app.routes.retencao import router as retencao_router\n"
    insert_include = "app.include_router(retencao_router)\n"

    # Adiciona import junto com os outros imports de routes
    if "from app.routes.upgrade import" in main_content:
        main_content = main_content.replace(
            "from app.routes.upgrade import",
            f"{insert_import}from app.routes.upgrade import"
        )
    else:
        main_content = insert_import + main_content

    # Adiciona include_router junto com os outros
    if "app.include_router(upgrade_router)" in main_content:
        main_content = main_content.replace(
            "app.include_router(upgrade_router)",
            f"app.include_router(upgrade_router)\n{insert_include}"
        )
    else:
        main_content += f"\n{insert_include}"

    with open(main_path, "w") as f:
        f.write(main_content)
    print("   ✅ main.py atualizado!\n")

# ── 4. Reinicia serviço ───────────────────────────────────────────────────────
print("4️⃣  Reiniciando hubcomercial_cliquedf...")
ret = os.system("systemctl restart hubcomercial_cliquedf")
if ret == 0:
    print("   ✅ Serviço reiniciado!\n")
else:
    print("   ⚠️  Erro ao reiniciar — verifique manualmente.\n")

# ── 5. Testa endpoint ─────────────────────────────────────────────────────────
import time, urllib.request, json
time.sleep(3)
print("5️⃣  Testando endpoint /api/painel/retencao/meses ...")
try:
    # Só testa se responde (sem token, espera 401)
    req = urllib.request.Request("http://localhost:8004/api/painel/retencao/meses")
    try:
        urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print("   ✅ Endpoint respondeu 401 (não autenticado) — rota registrada!\n")
        else:
            print(f"   ⚠️  HTTP {e.code} — verifique o log.\n")
    except Exception as ex:
        print(f"   ⚠️  {ex}\n")
except Exception as ex:
    print(f"   ⚠️  {ex}\n")

print("=" * 50)
print("✅ Instalação do backend concluída!")
print("   Próximo passo: adicionar a página no painel.html")
print("=" * 50)
