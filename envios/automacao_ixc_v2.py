import requests
import base64
from pathlib import Path
from datetime import datetime

# --- CONFIGURAÇÕES ---
HOST = 'sistema.cliquedf.com.br'
URL = f"https://{HOST}/webservice/v1/cliente_arquivos"
TOKEN = "64:90b12b22159c00f223eb3e0411f3f1999f68098d1a27127dbec670997ddd800c".encode('utf-8')

auth_base64 = base64.b64encode(TOKEN).decode('utf-8')
headers = {
    'ixcsoft': 'gravar',
    'Authorization': f'Basic {auth_base64}'
}

# Caminho nano+path do arquivo no seu VPS
LOCAL_FILE = Path("/opt/automacoes/cliquedf/comercial/uploads/61/rg_frente.jpg")

# --- AQUI ESTÁ A DEFINIÇÃO ---
def upload_vps_para_ixc_real(id_cliente):
    if not LOCAL_FILE.exists():
        print(f"❌ Arquivo {LOCAL_FILE} não encontrado no VPS!")
        return

    nome_arquivo = LOCAL_FILE.name 

    try:
        with open(LOCAL_FILE, 'rb') as f:
            files = {'arquivo': (nome_arquivo, f, 'image/jpeg')}
            
            payload = {
                'id_cliente': str(id_cliente),
                'descricao': 'SELFIE_VPS_TRANSFER_OK',
                'nome_arquivo': nome_arquivo,
                'local_arquivo': f'arquivos/{nome_arquivo}',
                'data_envio': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'tipo_arquivo': 'C'
            }

            print(f"🚀 Enviando do VPS para o IXC...")
            response = requests.post(URL, data=payload, files=files, headers=headers)

            if response.status_code == 200:
                print(f"✅ Sucesso! Resposta: {response.text}")
            else:
                print(f"❌ Erro: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"💥 Falha: {e}")

# --- AQUI ESTÁ A CHAMADA (PRECISA SER O MESMO NOME) ---
if __name__ == "__main__":
    upload_vps_para_ixc_real('12532')
