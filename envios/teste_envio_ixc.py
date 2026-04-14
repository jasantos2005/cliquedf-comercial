import requests
import base64
import json
from pathlib import Path

# --- CONFIGURAÇÕES ---
HOST = 'sistema.cliquedf.com.br'
URL = f"https://{HOST}/webservice/v1/cliente_arquivos"
TOKEN = "64:90b12b22159c00f223eb3e0411f3f1999f68098d1a27127dbec670997ddd800c".encode('utf-8')

# Autenticação codificada
auth_base64 = base64.b64encode(TOKEN).decode('utf-8')

headers = {
    'ixcsoft': 'gravar',
    'Authorization': f'Basic {auth_base64}',
    'Content-Type': 'application/json'
}

# Path do arquivo no VPS
LOCAL_FILE = Path("/opt/automacoes/cliquedf/comercial/uploads/61/rg_frente.jpg")

def upload_base64_ixc(id_cliente):
    if not LOCAL_FILE.exists():
        print(f"❌ Arquivo não encontrado: {LOCAL_FILE}")
        return

    try:
        # 1. Converte o arquivo físico para uma string Base64
        with open(LOCAL_FILE, "rb") as image_file:
            # Lendo os bytes e convertendo para string base64
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')

        # 2. Monta o JSON conforme o IXC espera para processar o binário
        payload = {
            'id_cliente': str(id_cliente),
            'descricao': 'RG FRENTE VIA BASE64',
            'nome_arquivo': LOCAL_FILE.name,
            'arquivo': encoded_string, # O binário vai aqui como texto
            'tipo_arquivo': 'C'
        }

        print(f"🚀 Enviando arquivo ({LOCAL_FILE.name}) via Base64...")
        
        # 3. Envia como um POST JSON normal
        response = requests.post(URL, data=json.dumps(payload), headers=headers)

        if response.status_code == 200:
            print("✅ Resposta do Servidor:", response.text)
            print("👉 Verifique agora se o campo 'Anexar arquivo' está preenchido no IXC.")
        else:
            print(f"❌ Erro {response.status_code}: {response.text}")

    except Exception as e:
        print(f"💥 Falha: {e}")

if __name__ == "__main__":
    upload_base64_ixc('12578')
