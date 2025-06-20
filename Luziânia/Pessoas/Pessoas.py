import pandas as pd
import requests
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def programa():
    API_URL_BASE = f"https://api.f10.com.br/unidade/pessoas/831"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGVzc2FuZHJvQGdhbWEuY29tLmJyIiwiaXNzIjoiRjEwIEFQSSIsImlhdCI6MTc0ODAwOTkyMSwiZXhwIjoyMDMyMDI4MzIxfQ.gNHt7U1x97tzKAsNajrBK92eByq9WGKafFAn1uirSyc"
    
    CAMINHO_CSV = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Luziânia/Pessoas/Dados_Pessoas.csv"
    CAMINHO_CREDENCIAL = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/n8n-credenciais-459013-3016ec1fb1f1.json"
    PLANILHA_ID = "1z4IoxPNgEnL0hLgC5eriOkINmHfcjm99lsZRoE2UYq0"
    NOME_ABA = "Pessoas"

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

        response = requests.get(API_URL_BASE, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Erro na API: {response.status_code} - {response.text}")
            break

        dados = response.json()
        if not dados:
            break

        todos_dados.extend(dados)
        last_id = dados[-1].get('pessoa_id')

        if len(dados) < 100:
            break

    print(f"✅ Total de pessoas recebidas: {len(todos_dados)}")

    novos_dados = []
    for pessoa in todos_dados:
        novos_dados.append({
            'Pessoa ID': "'" + str(pessoa.get('pessoa_id')),
            'Nome': pessoa.get('nome'),
            'CPF/CNPJ': pessoa.get('cpf_cnpj'),
            'RG/IE': pessoa.get('rg_ie'),
            'Nascimento': pessoa.get('nascimento'),
            'Email': pessoa.get('email'),
            'Telefone': pessoa.get('telefone'),
            'Celular': pessoa.get('celular'),
            'Logradouro': pessoa.get('logradouro'),
            'Bairro': pessoa.get('bairro'),
            'Complemento': pessoa.get('end_complemento'),
            'Cidade': pessoa.get('cidade'),
            'Estado': pessoa.get('estado'),
            'CEP': pessoa.get('cep')
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

    return df_final 
