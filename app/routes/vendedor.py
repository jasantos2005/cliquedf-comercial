"""Hub Comercial — app/routes/vendedor.py"""
import sqlite3, json, uuid, logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, File, UploadFile, Form, Query, Request
from app.services.auth import requer_vendedor
from app.services.ixc_db import ixc_select, ixc_select_one

BASE_DIR   = Path(__file__).resolve().parent.parent.parent
DB_PATH    = BASE_DIR / "hub_comercial.db"
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
log = logging.getLogger(__name__); router = APIRouter()

def get_db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False); c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    try: yield c
    finally: c.close()

def agora(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@router.get("/planos")
async def listar_planos(db=Depends(get_db), user=Depends(requer_vendedor())):
    rows = db.execute("SELECT id,nome,descricao,valor,taxa_instalacao,fidelidade FROM hc_planos WHERE ativo='S' ORDER BY nome").fetchall()
    return {"planos": [dict(r) for r in rows]}

@router.get("/cidade-por-ibge")
async def cidade_por_ibge(ibge: str=Query(...), user=Depends(requer_vendedor())):
    row = ixc_select_one("SELECT id,nome,uf FROM ixcprovedor.cidade WHERE cod_ibge=%s LIMIT 1",(ibge,))
    return {"id": row["id"] if row else None, "nome": row["nome"] if row else None}

@router.get("/cidade-por-cep")
async def cidade_por_cep(cep: str=Query(...), user=Depends(requer_vendedor()), db=Depends(get_db)):
    """Busca cidade pela faixa de CEP na tabela local hc_cidades."""
    cep_num = cep.replace("-","").replace(".","").strip()
    if len(cep_num) != 8:
        raise HTTPException(400, "CEP inválido.")
    row = db.execute("""
        SELECT ixc_id as id, nome, uf_sigla
        FROM hc_cidades
        WHERE cep_inicio <= ? AND cep_fim >= ?
        LIMIT 1
    """, (cep_num, cep_num)).fetchone()
    if not row:
        return {"id": None, "nome": None, "uf_sigla": None}
    return dict(row)

@router.get("/viabilidade")
async def verificar_viabilidade(cep:str=Query(...),endereco:str=Query(""),numero:str=Query(""),bairro:str=Query(""),cidade:str=Query(""),user=Depends(requer_vendedor())):
    c = cep.replace("-","").strip(); itens=[]
    r2=ixc_select("SELECT COUNT(*) AS qtd FROM ixcprovedor.cliente c JOIN ixcprovedor.cliente_contrato ct ON ct.id_cliente=c.id AND ct.status='A' WHERE REPLACE(c.cep,'-','')=%s",(c,))
    q2=r2[0]["qtd"] if r2 else 0
    if q2>0:
        if numero and numero.upper()!="SN":
            r1=ixc_select("SELECT COUNT(*) AS qtd FROM ixcprovedor.cliente c JOIN ixcprovedor.cliente_contrato ct ON ct.id_cliente=c.id AND ct.status='A' WHERE REPLACE(c.cep,'-','')=%s AND UPPER(TRIM(c.numero))=UPPER(TRIM(%s))",(c,numero))
            if r1 and r1[0]["qtd"]>0: itens.append(f"Atenção: {r1[0]['qtd']} cliente(s) ativo(s) neste endereço exato")
        itens.append(f"{q2} cliente(s) ativo(s) no mesmo CEP — cobertura confirmada")
        return {"status":"ok","nivel":2,"itens":itens}
    if bairro and cidade:
        r3=ixc_select("SELECT COUNT(*) AS qtd FROM ixcprovedor.cliente c JOIN ixcprovedor.cliente_contrato ct ON ct.id_cliente=c.id AND ct.status='A' WHERE UPPER(TRIM(c.bairro))=UPPER(TRIM(%s)) AND c.cidade IN (SELECT id FROM ixcprovedor.cidade WHERE UPPER(nome)=UPPER(%s))",(bairro,cidade))
        if r3 and r3[0]["qtd"]>0:
            itens.append(f"{r3[0]['qtd']} cliente(s) no mesmo bairro")
            return {"status":"ok","nivel":3,"itens":itens}
    if cidade:
        r4=ixc_select("SELECT COUNT(*) AS qtd FROM ixcprovedor.cliente c JOIN ixcprovedor.cliente_contrato ct ON ct.id_cliente=c.id AND ct.status='A' WHERE c.cidade IN (SELECT id FROM ixcprovedor.cidade WHERE UPPER(nome)=UPPER(%s))",(cidade,))
        if r4 and r4[0]["qtd"]>0:
            itens+=["Cidade atendida, sem histórico neste bairro/CEP","Confirme com supervisor antes de prosseguir"]
            return {"status":"alerta","nivel":4,"itens":itens}
    return {"status":"bloqueado","nivel":0,"itens":["Nenhum cliente ativo nesta cidade","Região fora da área de atendimento"]}


@router.get("/consultar-cpf/{cpf}")
def consultar_cpf(cpf: str, user=Depends(requer_vendedor())):
    """
    Consulta CPF/CNPJ no IXC e retorna o cenário:
    - livre: CPF não existe na base
    - novo_contrato: cliente ativo, pode criar novo contrato
    - ex_cliente_ok: ex-cliente sem dívidas, pode prosseguir
    - bloqueado: ex-cliente com dívidas, não pode prosseguir
    """
    from app.services.ixc_db import ixc_conn
    import re
    cpf_limpo = re.sub(r'[^0-9]', '', cpf)
    # Formatar com máscara para busca
    if len(cpf_limpo) == 11:
        cpf_fmt = f"{cpf_limpo[:3]}.{cpf_limpo[3:6]}.{cpf_limpo[6:9]}-{cpf_limpo[9:]}"
    elif len(cpf_limpo) == 14:
        cpf_fmt = f"{cpf_limpo[:2]}.{cpf_limpo[2:5]}.{cpf_limpo[5:8]}/{cpf_limpo[8:12]}-{cpf_limpo[12:]}"
    else:
        cpf_fmt = cpf_limpo

    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            # Buscar cliente por CPF (com ou sem máscara)
            cur.execute("""
                SELECT c.id, c.razao, c.ativo, c.cidade,
                       ci.nome as cidade_nome,
                       cc.status as contrato_status,
                       cc.id as contrato_id
                FROM cliente c
                LEFT JOIN cidade ci ON ci.id = c.cidade
                LEFT JOIN cliente_contrato cc ON cc.id_cliente = c.id AND cc.status = 'A'
                WHERE c.cnpj_cpf IN (%s, %s)
                ORDER BY c.id DESC LIMIT 5
            """, (cpf_fmt, cpf_limpo))
            clientes = cur.fetchall()

            if not clientes:
                return {"cenario": "livre", "msg": "CPF não encontrado na base. Pode prosseguir com o cadastro."}

            # Pegar cliente mais recente
            cli = clientes[0]

            # Verificar se tem contrato ativo
            tem_ativo = any(r["contrato_status"] == "A" for r in clientes)

            if tem_ativo:
                # Cenário 2: cliente ativo — novo contrato
                contratos_ativos = [r for r in clientes if r["contrato_status"] == "A"]
                return {
                    "cenario": "novo_contrato",
                    "msg": f"Cliente já cadastrado e ativo. Será criado apenas um novo contrato.",
                    "ixc_cliente_id": cli["id"],
                    "razao": cli["razao"],
                    "cidade": cli["cidade_nome"] or "",
                    "contratos_ativos": len(contratos_ativos)
                }

            # Cliente inativo — verificar dívidas
            cur.execute("""
                SELECT COUNT(*) as qtd, SUM(valor_aberto) as total
                FROM fn_areceber
                WHERE id_cliente = %s AND status = 'A' AND valor_aberto > 0
            """, (cli["id"],))
            divida = cur.fetchone()
            tem_divida = divida and divida["qtd"] > 0 and float(divida["total"] or 0) > 0

            if tem_divida:
                return {
                    "cenario": "bloqueado",
                    "msg": f"Cliente com dívidas em aberto. Não é possível prosseguir.",
                    "ixc_cliente_id": cli["id"],
                    "razao": cli["razao"],
                    "divida_qtd": int(divida["qtd"]),
                    "divida_total": float(divida["total"] or 0)
                }

            # Ex-cliente sem dívidas
            return {
                "cenario": "ex_cliente_ok",
                "msg": f"Ex-cliente encontrado sem dívidas. Pode prosseguir com novo cadastro.",
                "ixc_cliente_id": cli["id"],
                "razao": cli["razao"],
                "cidade": cli["cidade_nome"] or ""
            }

    except Exception as e:
        raise HTTPException(500, f"Erro ao consultar CPF: {e}")

@router.post("/precadastro")
async def criar_precadastro(dados:str=Form(...),rg:UploadFile=File(None),selfie:UploadFile=File(None),comp:UploadFile=File(None),db=Depends(get_db),user=Depends(requer_vendedor())):
    try: f=json.loads(dados)
    except: raise HTTPException(400,"Dados inválidos.")
    if not rg or not selfie: raise HTTPException(400,"RG e selfie são obrigatórios.")
    protocolo=f"HC{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:4].upper()}"
    cur=db.cursor()
    cur.execute("""INSERT INTO hc_precadastros(status,id_vendedor_hub,ixc_vendedor_id,canal_venda,protocolo,tipo_pessoa,razao,cnpj_cpf,telefone_celular,whatsapp,email,data_nascimento,sexo,rg_orgao_emissor,nacionalidade,cep,endereco,numero,bairro,complemento,referencia,cidade_nome,uf_sigla,ixc_cidade_id,viabilidade_status,viabilidade_nivel,viabilidade_obs,viabilidade_checado_em,ixc_plano_id,plano_nome,plano_valor,taxa_instalacao,fidelidade,dia_vencimento,obs,criado_em,atualizado_em)VALUES('enviado',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','-3 hours'),datetime('now','-3 hours'))""",
        (int(user["sub"]),user.get("ixc_funcionario_id"),f.get("canal_venda"),protocolo,f.get("tipo_pessoa","F"),f.get("razao"),f.get("cnpj_cpf"),f.get("telefone_celular"),f.get("whatsapp"),f.get("email"),f.get("data_nascimento") or None,f.get("sexo") or "",f.get("rg_orgao_emissor") or "",f.get("nacionalidade") or "Brasileiro",f.get("cep"),f.get("endereco"),f.get("numero"),f.get("bairro"),f.get("complemento"),f.get("referencia"),f.get("cidade_nome"),f.get("uf_sigla"),f.get("ixc_cidade_id"),f.get("viabilidade_status"),f.get("viabilidade_nivel",0),f.get("viabilidade_obs"),agora(),f.get("ixc_plano_id"),f.get("plano_nome"),f.get("plano_valor"),f.get("taxa_instalacao"),f.get("fidelidade"),f.get("dia_vencimento"),f.get("obs")))
    pid=cur.lastrowid; db.commit()
    pasta=UPLOAD_DIR/str(pid); pasta.mkdir(exist_ok=True)
    async def salvar(file,tipo):
        if not file: return
        ext=Path(file.filename or "").suffix or ".jpg"; dest=pasta/f"{tipo}{ext}"
        raw = await file.read()
        try:
            from PIL import Image as _Img, ImageEnhance as _IE
            import io as _io
            img = _Img.open(_io.BytesIO(raw))
            # Converter para RGB com fundo branco
            bg = _Img.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'RGBA':
                bg.paste(img, mask=img.split()[3])
            else:
                bg.paste(img.convert('RGB'))
            img = bg
            # Corrigir brilho se imagem estiver escura
            pixels = list(img.getdata())
            media = sum(sum(p) for p in pixels[:200]) / (200 * 3)
            if media < 80:  # imagem escura — aumentar brilho
                img = _IE.Brightness(img).enhance(3.0)
                img = _IE.Contrast(img).enhance(1.2)
            # Redimensionar
            MAX = 1200
            w, h = img.size
            if w > MAX or h > MAX:
                ratio = min(MAX/w, MAX/h)
                img = img.resize((int(w*ratio), int(h*ratio)), _Img.LANCZOS)
            buf = _io.BytesIO()
            img.save(buf, 'JPEG', quality=80)
            dest.write_bytes(buf.getvalue())
        except Exception:
            dest.write_bytes(raw)
        kb = dest.stat().st_size // 1024
        cur.execute("INSERT INTO hc_precadastro_docs(precadastro_id,tipo,arquivo,tamanho_kb)VALUES(?,?,?,?)",(pid,tipo,f"uploads/{pid}/{tipo}{ext}",kb)); db.commit()
    await salvar(rg,"rg_frente"); await salvar(selfie,"selfie_doc"); await salvar(comp,"comp_residencia")
    log.info(f"Pré-cadastro #{pid} protocolo={protocolo}")
    return {"id":pid,"protocolo":protocolo,"status":"enviado"}

@router.get("/meus-cadastros")
async def meus_cadastros(db=Depends(get_db),user=Depends(requer_vendedor())):
    rows=db.execute("""SELECT p.id,p.protocolo,p.status,p.razao,p.cnpj_cpf,p.plano_nome,p.cidade_nome,p.uf_sigla,p.criado_em,p.atualizado_em,(SELECT GROUP_CONCAT(a.legenda,' | ')FROM hc_auditoria_log a WHERE a.precadastro_id=p.id AND a.resultado IN('reprovado','pendente')ORDER BY a.id DESC LIMIT 3)AS motivo_auditoria FROM hc_precadastros p WHERE p.id_vendedor_hub=? ORDER BY p.criado_em DESC LIMIT 50""",(int(user["sub"]),)).fetchall()
    return {"cadastros":[dict(r) for r in rows]}

# ── CORRIGIR PRÉ-CADASTRO (devolvido pelo backoffice) ────────
@router.put("/precadastro/{id}/corrigir")
async def corrigir_precadastro(
    id: int,
    request: Request,
    db=Depends(get_db),
    user=Depends(requer_vendedor())
):
    """
    Atualiza os dados de um pré-cadastro devolvido para correção.
    Só o vendedor dono do cadastro pode corrigir.
    Volta o status para 'enviado' automaticamente.
    """
    vid = int(user["sub"])
    p = db.execute(
        "SELECT id, status, id_vendedor_hub FROM hc_precadastros WHERE id=? AND id_vendedor_hub=?",
        (id, vid)
    ).fetchone()
    if not p:
        raise HTTPException(404, "Cadastro não encontrado.")
    if p["status"] != "aguard_correcao":
        raise HTTPException(400, f"Cadastro não está aguardando correção. Status: {p['status']}")

    body = await request.json()
    f = body.get("dados", body)  # aceita tanto {dados: {...}} quanto o objeto direto

    db.execute("""
        UPDATE hc_precadastros SET
            tipo_pessoa=?, razao=?, cnpj_cpf=?, telefone_celular=?, whatsapp=?,
            email=?, data_nascimento=?,
            sexo=?, rg_orgao_emissor=?, nacionalidade=?,
            cep=?, endereco=?, numero=?, bairro=?, complemento=?, referencia=?,
            cidade_nome=?, uf_sigla=?, ixc_cidade_id=?,
            ixc_plano_id=?, plano_nome=?, plano_valor=?, taxa_instalacao=?,
            fidelidade=?, dia_vencimento=?, obs=?,
            status='enviado',
            atualizado_em=datetime('now','-3 hours')
        WHERE id=? AND id_vendedor_hub=?
    """, (
        f.get("tipo_pessoa","F"), f.get("razao"), f.get("cnpj_cpf"),
        f.get("telefone_celular"), f.get("whatsapp"),
        f.get("email"), f.get("data_nascimento") or None,
        f.get("sexo") or "", f.get("rg_orgao_emissor") or "", f.get("nacionalidade") or "Brasileiro",
        f.get("cep"), f.get("endereco"), f.get("numero"),
        f.get("bairro"), f.get("complemento"), f.get("referencia"),
        f.get("cidade_nome"), f.get("uf_sigla"), f.get("ixc_cidade_id"),
        f.get("ixc_plano_id"), f.get("plano_nome"), f.get("plano_valor"),
        f.get("taxa_instalacao"), f.get("fidelidade"), f.get("dia_vencimento"), f.get("obs"),
        id, vid
    ))
    # Registrar no log de auditoria
    db.execute("""
        INSERT INTO hc_auditoria_log(precadastro_id,rodada,regra,legenda,resultado,detalhes)
        VALUES(?,99,'CORRECAO','Cadastro corrigido pelo vendedor','ok',?)
    """, (id, f"Corrigido por {user['login']} e reenviado para auditoria"))
    db.commit()

    return {"ok": True, "status": "enviado", "msg": "Cadastro corrigido e enviado para auditoria!"}


# ── CIDADES PARA CACHE OFFLINE ────────────────────────────────
@router.get("/cidades-cache")
async def cidades_cache(user=Depends(requer_vendedor())):
    from app.services.ixc_db import ixc_conn
    try:
        with ixc_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ci.id, ci.nome, ci.uf, u.sigla as uf_sigla
                FROM ixcprovedor.cidade ci
                JOIN ixcprovedor.cliente cl ON cl.cidade = ci.id
                LEFT JOIN ixcprovedor.uf u ON u.id = ci.uf
                WHERE cl.ativo = 'S'
                ORDER BY ci.nome
            """)
            return {"cidades": [dict(r) for r in cur.fetchall()]}
    except Exception as e:
        raise HTTPException(500, str(e))
