import pandas as pd
import requests
import gspread
import os
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def programa():
    cursos_ids = [52, 53, 54, 55, 56, 57, 58, 2000]

    valor = cursos_ids[-1]
    for i in range(1, valor + 1):

        UNIDADE_ID = 831
        API_URL = f"https://api.f10.com.br/unidade/turmas/{UNIDADE_ID}/{i}/pautas"
        API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGVzc2FuZHJvQGdhbWEuY29tLmJyIiwiaXNzIjoiRjEwIEFQSSIsImlhdCI6MTc0ODAwOTkyMSwiZXhwIjoyMDMyMDI4MzIxfQ.gNHt7U1x97tzKAsNajrBK92eByq9WGKafFAn1uirSyc"

        CAMINHO_CSV = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Luziânia/Turmas/Dados_Das_Lista_De_Pautas_Das_Turmas.csv"
        CAMINHO_CREDENCIAL = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/n8n-credenciais-459013-3016ec1fb1f1.json"
        PLANILHA_ID = "1z4IoxPNgEnL0hLgC5eriOkINmHfcjm99lsZRoE2UYq0"
        NOME_ABA = "Presenças Das Turmas"

        ORDEM_COLUNAS = [
            'Pauta ID', 'Turma ID', 'Turma', 'Matéria ID', 'Matéria',
            'Professor ID', 'Professor', 'Data', 'Tempo',
            'Contrato ID', 'Aluno ID', 'Presente',
            'Justificativa', 'Reposição'
        ]

        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }

        try:
            print(f"\n🔍 Requisitando pautas da turma {i}...")
            response = requests.get(API_URL, headers=headers, timeout=30)
            response.raise_for_status()

            pautas = response.json()
            if not pautas:
                print("⚠ Nenhuma pauta encontrada.")
                continue

            dados_formatados = []
            for pauta in pautas:
                for presenca in pauta.get('presencas', []): 
                    dados_formatados.append({
                        'Pauta ID': pauta.get('pauta_id', ''),
                        'Turma ID': pauta.get('turma_id', ''),
                        'Turma': pauta.get('turma', ''),
                        'Matéria ID': pauta.get('materia_id', ''),
                        'Matéria': pauta.get('materia', ''),
                        'Professor ID': pauta.get('professor_id', ''),
                        'Professor': pauta.get('professor', ''),
                        'Data': pauta.get('data', ''),
                        'Tempo': pauta.get('tempo', ''),
                        'Contrato ID': presenca.get('contrato_id', ''),
                        'Aluno ID': presenca.get('aluno_id', ''),
                        'Presente': presenca.get('presente', ''),
                        'Justificativa': presenca.get('justificativa', ''),
                        'Reposição': presenca.get('reposicao', '')
                    })

            df_novo = pd.DataFrame(dados_formatados)[ORDEM_COLUNAS]
            print(f"📊 {len(df_novo)} registros formatados")

            os.makedirs(os.path.dirname(CAMINHO_CSV), exist_ok=True)

            if os.path.exists(CAMINHO_CSV):
                try:
                    df_existente = pd.read_csv(CAMINHO_CSV, dtype=str)
                    df_existente = df_existente.reindex(columns=ORDEM_COLUNAS, fill_value='')
                    df_final = pd.concat([df_existente, df_novo]).drop_duplicates(subset=['Pauta ID', 'Aluno ID'], keep='last')
                    print(f"✅ CSV existente carregado com {len(df_existente)} registros")
                except pd.errors.EmptyDataError:
                    df_final = df_novo
                    print("⚠ CSV existente estava vazio, criando novo")
            else:
                df_final = df_novo
                print("🆕 Criando novo arquivo CSV")

            df_final.to_csv(CAMINHO_CSV, index=False, encoding='utf-8-sig')
            print(f"✔ CSV salvo com {len(df_final)} registros em: {CAMINHO_CSV}")

            print("\n🌐 Atualizando Google Sheets...")
            escopo = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            credenciais = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_CREDENCIAL, escopo)
            cliente = gspread.authorize(credenciais)

            planilha = cliente.open_by_key(PLANILHA_ID)
            try:
                aba = planilha.worksheet(NOME_ABA)
            except gspread.WorksheetNotFound:
                print(f"⚠ Aba '{NOME_ABA}' não encontrada. Criando nova...")
                aba = planilha.add_worksheet(title=NOME_ABA, rows=1000, cols=20)

            aba.clear()
            set_with_dataframe(aba, df_final[ORDEM_COLUNAS], include_index=False, include_column_header=True, resize=True)
            print("✔ Planilha atualizada com sucesso!")

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição HTTP: {str(e)}")
        except Exception as e:
            print(f"❌ Erro inesperado: {str(e)}")
        finally:
            print("🏁 Execução concluída")

    return df_final
