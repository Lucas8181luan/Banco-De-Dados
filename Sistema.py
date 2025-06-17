import pandas as pd
import time
from openpyxl import load_workbook

def DEZ_MIN(acao):
    while True:
        acao() 
        time.sleep(600)

def programa():
    # Caminho do arquivo
    caminho = "C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Dados.xlsx"
    
    # Coletar dados
    nome = input("Nome: ")
    idade = input("Idade: ")
    email = input("E-mail: ")
    curso = input("Curso: ")
    
    # Verificar se o arquivo já existe
    try:
        # Carrega dados existentes
        dados_existentes = pd.read_excel(caminho)
        # Adiciona novo registro
        novo_dado = pd.DataFrame([{
            'Nome': nome,
            'Idade': idade,
            'E-mail': email,
            'Curso': curso
        }])
        # Combina dados
        dados_atualizados = pd.concat([dados_existentes, novo_dado], ignore_index=True)
    except FileNotFoundError:
        # Se o arquivo não existe, cria novo DataFrame
        dados_atualizados = pd.DataFrame([{
            'Nome': nome,
            'Idade': idade,
            'E-mail': email,
            'Curso': curso
        }])
    
    # Salva o arquivo
    dados_atualizados.to_excel(caminho, index=False)
    print("Dados adicionados com sucesso!")

# Para executar a cada 10 minutos:
# DEZ_MIN(programa)

# Para executar uma vez:
programa()