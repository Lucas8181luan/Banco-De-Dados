import pandas as pd
import requests
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def programa(data_inicial=None, data_final=None):
    API_URL_BASE = "https://api.f10.com.br/unidade/contratos/831"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGVzc2FuZHJvQGdhbWEuY29tLmJyIiwiaXNzIjoiRjEwIEFQSSIsImlhdCI6MTc0ODAwOTkyMSwiZXhwIjoyMDMyMDI4MzIxfQ.gNHt7U1x97tzKAsNajrBK92eByq9WGKafFAn1uirSyc"
    
    CAMINHO_CSV = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Luziânia/Dados_Contratos.csv"
    CAMINHO_CREDENCIAL = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/n8n-credenciais-459013-3016ec1fb1f1.json"
    PLANILHA_ID = "1z4IoxPNgEnL0hLgC5eriOkINmHfcjm99lsZRoE2UYq0"
    NOME_ABA = "Contratos"

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    todos_dados = []
    last_id = None

    while True:
        params = {}
        if last_id:
            params['lastId'] = last_id
        if data_inicial:
            params['dataInicial'] = data_inicial
        if data_final:
            params['dataFinal'] = data_final
        
        response = requests.get(API_URL_BASE, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Erro na API: {response.status_code} - {response.text}")
            break
        
        dados = response.json()
        if not dados:
            break
        
        todos_dados.extend(dados)
        last_id = dados[-1].get('contrato_id')

        if len(dados) < 100:
            break

    print(f"✅ Total de contratos recebidos: {len(todos_dados)}")

    novos_dados = []
    for contrato in todos_dados:
        novos_dados.append({
            'Contrato ID': contrato.get('contrato_id'),
            'Número Contrato': contrato.get('numero_contrato'),
            'Status': contrato.get('x_status'),
            'Matrícula': contrato.get('matricula'),
            'Cancelamento': contrato.get('data_cancelamento'),
            'Curso ID': contrato.get('curso_id'),
            'Fonte': contrato.get('fonte'),
            'Pessoa ID': contrato.get('pessoa_id'),
            'Titular ID': contrato.get('titular_id'),
            'Vendedor ID': contrato.get('vendedor_id'),
            'Telemarketing ID': contrato.get('telemarketing_id')
        })

    df_novos = pd.DataFrame(novos_dados)

    try:
        df_existente = pd.read_csv(CAMINHO_CSV)
        df_final = pd.concat([df_existente, df_novos], ignore_index=True).drop_duplicates()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df_final = df_novos

    df_final.to_csv(CAMINHO_CSV, index=False, encoding='utf-8-sig')
    print(f"✅ CSV salvo com {len(df_final)} registros")

    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_CREDENCIAL, escopo)
    cliente = gspread.authorize(credenciais)
    planilha = cliente.open_by_key(PLANILHA_ID)
    aba = planilha.worksheet(NOME_ABA)

    aba.clear()
    set_with_dataframe(aba, df_final)
    print(f"✅ Planilha atualizada com {len(df_final)} registros")

# Executa puxando tudo (sem filtro)
programa()
