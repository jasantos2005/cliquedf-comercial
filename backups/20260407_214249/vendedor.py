"""Hub Comercial — app/routes/vendedor.py"""
import sqlite3, json, uuid, logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, File, UploadFile, Form, Query
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

@router.post("/precadastro")
async def criar_precadastro(dados:str=Form(...),rg:UploadFile=File(None),selfie:UploadFile=File(None),comp:UploadFile=File(None),db=Depends(get_db),user=Depends(requer_vendedor())):
    try: f=json.loads(dados)
    except: raise HTTPException(400,"Dados inválidos.")
    if not rg or not selfie: raise HTTPException(400,"RG e selfie são obrigatórios.")
    protocolo=f"HC{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:4].upper()}"
    cur=db.cursor()
    cur.execute("""INSERT INTO hc_precadastros(status,id_vendedor_hub,ixc_vendedor_id,canal_venda,protocolo,tipo_pessoa,razao,cnpj_cpf,telefone_celular,whatsapp,email,data_nascimento,cep,endereco,numero,bairro,complemento,referencia,cidade_nome,uf_sigla,ixc_cidade_id,viabilidade_status,viabilidade_nivel,viabilidade_obs,viabilidade_checado_em,ixc_plano_id,plano_nome,plano_valor,taxa_instalacao,fidelidade,dia_vencimento,obs,criado_em,atualizado_em)VALUES('enviado',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','-3 hours'),datetime('now','-3 hours'))""",
        (int(user["sub"]),user.get("ixc_funcionario_id"),f.get("canal_venda"),protocolo,f.get("tipo_pessoa","F"),f.get("razao"),f.get("cnpj_cpf"),f.get("telefone_celular"),f.get("whatsapp"),f.get("email"),f.get("data_nascimento") or None,f.get("cep"),f.get("endereco"),f.get("numero"),f.get("bairro"),f.get("complemento"),f.get("referencia"),f.get("cidade_nome"),f.get("uf_sigla"),f.get("ixc_cidade_id"),f.get("viabilidade_status"),f.get("viabilidade_nivel",0),f.get("viabilidade_obs"),agora(),f.get("ixc_plano_id"),f.get("plano_nome"),f.get("plano_valor"),f.get("taxa_instalacao"),f.get("fidelidade"),f.get("dia_vencimento"),f.get("obs")))
    pid=cur.lastrowid; db.commit()
    pasta=UPLOAD_DIR/str(pid); pasta.mkdir(exist_ok=True)
    async def salvar(file,tipo):
        if not file: return
        ext=Path(file.filename or "").suffix or ".jpg"; dest=pasta/f"{tipo}{ext}"
        dest.write_bytes(await file.read()); kb=dest.stat().st_size//1024
        cur.execute("INSERT INTO hc_precadastro_docs(precadastro_id,tipo,arquivo,tamanho_kb)VALUES(?,?,?,?)",(pid,tipo,f"uploads/{pid}/{tipo}{ext}",kb)); db.commit()
    await salvar(rg,"rg_frente"); await salvar(selfie,"selfie_doc"); await salvar(comp,"comp_residencia")
    log.info(f"Pré-cadastro #{pid} protocolo={protocolo}")
    return {"id":pid,"protocolo":protocolo,"status":"enviado"}

@router.get("/meus-cadastros")
async def meus_cadastros(db=Depends(get_db),user=Depends(requer_vendedor())):
    rows=db.execute("""SELECT p.id,p.protocolo,p.status,p.razao,p.cnpj_cpf,p.plano_nome,p.cidade_nome,p.uf_sigla,p.criado_em,p.atualizado_em,(SELECT GROUP_CONCAT(a.legenda,' | ')FROM hc_auditoria_log a WHERE a.precadastro_id=p.id AND a.resultado IN('reprovado','pendente')ORDER BY a.id DESC LIMIT 3)AS motivo_auditoria FROM hc_precadastros p WHERE p.id_vendedor_hub=? ORDER BY p.criado_em DESC LIMIT 50""",(int(user["sub"]),)).fetchall()
    return {"cadastros":[dict(r) for r in rows]}
