import pandas as pd
import requests
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def programa():
    API_URL_BASE = f"https://api.f10.com.br/unidade/turmas/831"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGVzc2FuZHJvQGdhbWEuY29tLmJyIiwiaXNzIjoiRjEwIEFQSSIsImlhdCI6MTc0ODAwOTkyMSwiZXhwIjoyMDMyMDI4MzIxfQ.gNHt7U1x97tzKAsNajrBK92eByq9WGKafFAn1uirSyc"
    
    CAMINHO_CSV = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Luziânia/Turmas/Dados_Lista_Turmas.csv"
    CAMINHO_CREDENCIAL = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/n8n-credenciais-459013-3016ec1fb1f1.json"
    PLANILHA_ID = "1z4IoxPNgEnL0hLgC5eriOkINmHfcjm99lsZRoE2UYq0"
    NOME_ABA = "Lista Turmas"

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
        last_id = dados[-1].get('turma_id')

        if len(dados) < 100:
            break

    print(f"✅ Total de turmas recebidas: {len(todos_dados)}")

    novos_dados = []
    for turma in todos_dados:
        novos_dados.append({
            'Turma ID': "'" + str(turma.get('turma_id')),
            'Turma': turma.get('turma'),
            'Curso ID': turma.get('curso_id'),
            'Professor ID': turma.get('professor_id'),
            'Capacidade': turma.get('capacidade'),
            'Matriculados': turma.get('matriculados'),
            'Incluir Alunos': turma.get('incluir_alunos'),
            'Sala de Aula': turma.get('sala_aula'),
            'Status': turma.get('status'),
            'Tipo': turma.get('tipo'),
            'Tipo Frequência': turma.get('tipo_frequencia'),
            'Data Abertura': turma.get('abertura'),
            'Data Início': turma.get('inicio'),
            'Previsão Término': turma.get('finalprevisao'),
            'Data Término Real': turma.get('finalreal'),
            'Matéria Atual ID': turma.get('materiaatual_id'),
            'Próxima Aula': turma.get('proximaaula'),
            'Carga Horária': turma.get('cargahoraria')
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

# Executa puxando tudo
programa()
