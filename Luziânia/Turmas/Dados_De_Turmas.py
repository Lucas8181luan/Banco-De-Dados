import pandas as pd
import requests
import gspread
import os
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path

def programa(V):
    # Configurações
    UNIDADE_ID = 831
    TURMA_ID = 913 
    API_URL = f"https://api.f10.com.br/unidade/turmas/{UNIDADE_ID}/{V}"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGVzc2FuZHJvQGdhbWEuY29tLmJyIiwiaXNzIjoiRjEwIEFQSSIsImlhdCI6MTc0ODAwOTkyMSwiZXhwIjoyMDMyMDI4MzIxfQ.gNHt7U1x97tzKAsNajrBK92eByq9WGKafFAn1uirSyc"
    
    # Definindo caminhos de forma robusta com Path
    BASE_DIR = Path("C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/Luziânia/Turmas")
    CAMINHO_CSV = BASE_DIR / "Dados_Das_Turmas.csv"
    CAMINHO_CREDENCIAL = Path("C:/Users/lucas/OneDrive/Documentos/GitHub/F10-Software/Banco-De-Dados/n8n-credenciais-459013-3016ec1fb1f1.json")
    PLANILHA_ID = "1z4IoxPNgEnL0hLgC5eriOkINmHfcjm99lsZRoE2UYq0"
    NOME_ABA = "Dados De Turmas"

    # Ordem das colunas (defina exatamente como deseja na saída)
    ORDEM_COLUNAS = [
        'Turma ID',
        'Turma',
        'Curso ID',
        'Professor ID',
        'Capacidade',
        'Matriculados',
        'Sala',
        'Status',
        'Tipo',
        'Início',
        'Término Previsto'
    ]

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        # 1. Obter dados da API
        print("\n🔍 Fazendo requisição à API...")
        response = requests.get(API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        turma = response.json()
        print("✅ Dados obtidos com sucesso:")
        print(turma)  # Mostra a resposta bruta para debug

        # 2. Processar e formatar os dados
        dados_formatados = {
            'Turma ID': str(turma.get('turma_id', '')).strip(),
            'Turma': turma.get('turma', '').strip() or 'Não informado',
            'Curso ID': str(turma.get('curso_id', '')).strip(),
            'Professor ID': str(turma.get('professor_id', '')).strip(),
            'Capacidade': str(turma.get('capacidade', '')).strip(),
            'Matriculados': str(turma.get('matriculados', '')).strip(),
            'Sala': (turma.get('sala_aula', '') or '').strip() or 'Não definida',
            'Status': (turma.get('status', '') or '').strip() or 'Não informado',
            'Tipo': (turma.get('tipo', '') or '').strip() or 'Não especificado',
            'Início': (turma.get('inicio', '') or '').strip() or 'Não definido',
            'Término Previsto': (turma.get('finalprevisao', '') or '').strip() or 'Não definido'
        }

        # 3. Criar DataFrame com ordem de colunas fixa
        df_novo = pd.DataFrame([dados_formatados])[ORDEM_COLUNAS]
        print("\n📊 DataFrame criado:")
        print(df_novo)

        # 4. Gerenciar arquivo CSV
        print("\n💾 Processando arquivo CSV...")
        BASE_DIR.mkdir(parents=True, exist_ok=True)  # Cria diretório se não existir
        
        if CAMINHO_CSV.exists():
            try:
                df_existente = pd.read_csv(CAMINHO_CSV, dtype=str)
                # Garante que as colunas existentes estejam na ordem correta
                df_existente = df_existente.reindex(columns=ORDEM_COLUNAS, fill_value='')
                # Remove duplicatas mantendo o último registro
                df_final = pd.concat([df_existente, df_novo]).drop_duplicates(subset=['Turma ID'], keep='last')
                print(f"✅ CSV existente carregado com {len(df_existente)} registros")
            except pd.errors.EmptyDataError:
                df_final = df_novo
                print("⚠ CSV existente estava vazio, criando novo")
        else:
            df_final = df_novo
            print("🆕 Criando novo arquivo CSV")

        # Salvar CSV formatado
        df_final.to_csv(CAMINHO_CSV, index=False, encoding='utf-8-sig')
        print(f"✔ CSV salvo com {len(df_final)} registros em: {CAMINHO_CSV}")

        # 5. Atualizar Google Sheets
        print("\n🌐 Conectando ao Google Sheets...")
        escopo = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        credenciais = ServiceAccountCredentials.from_json_keyfile_name(
            str(CAMINHO_CREDENCIAL), escopo)
        gc = gspread.authorize(credenciais)
        
        try:
            planilha = gc.open_by_key(PLANILHA_ID)
            try:
                aba = planilha.worksheet(NOME_ABA)
            except gspread.WorksheetNotFound:
                print(f"⚠ Aba '{NOME_ABA}' não encontrada, criando nova...")
                aba = planilha.add_worksheet(title=NOME_ABA, rows=100, cols=20)
            
            # Atualizar planilha mantendo formatação
            aba.clear()
            set_with_dataframe(aba, df_final[ORDEM_COLUNAS], include_index=False, include_column_header=True, resize=True)
            print("✔ Planilha atualizada com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro ao acessar Google Sheets: {str(e)}")
            raise

    except requests.exceptions.RequestException as e:
        print(f"\n❌ Erro na requisição HTTP: {str(e)}")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {str(e)}")
    finally:
        print("\n🏁 Execução concluída")

turmas_ids = [
        426, 427, 428, 429, 430, 433, 434, 435, 436, 437, 438, 439, 440,
        441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453,
        454, 455, 459, 460, 461, 463, 464, 465, 466, 467, 468, 469, 470,
        471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483,
        485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
        498, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 512,
        513, 514, 515, 516, 518, 519, 520, 521, 522, 524, 525, 526, 527,
        528, 529, 530, 531, 532, 533, 536, 537, 538, 539, 541, 542, 543,
        544, 545, 546, 547, 551, 552, 553, 554, 555, 556, 557, 558, 559,
        562, 563, 565, 566, 576, 577, 578, 579, 580, 581, 582, 584, 586,
        587, 588, 591, 592, 593, 594, 597, 598, 599, 600, 601, 603, 604,
        605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 627, 632, 633,
        634, 643, 644, 645, 648, 649, 659, 660, 661, 664, 665, 666, 667,
        668, 670, 671, 672, 673, 675, 676, 677, 680, 682, 683, 684, 686,
        687, 688, 689, 695, 696, 699, 700, 701, 702, 703, 704, 706, 707,
        708, 709, 710, 712, 713, 715, 718, 720, 721, 722, 723, 724, 725,
        726, 727, 731, 732, 733, 736, 738, 739, 740, 744, 745, 747, 748,
        753, 754, 755, 757, 759, 767, 768, 769, 770, 772, 773, 774, 775,
        776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788,
        789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801,
        802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814,
        815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827,
        828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840,
        841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853,
        854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866,
        867, 868, 869, 870, 871, 872, 873, 874, 875, 877, 878, 879, 880,
        881, 882, 883, 884, 885, 886, 887, 888, 889, 891, 892, 893, 894,
        895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907,
        908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920,
        921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933,
        934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946,
        947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959,
        960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 972,
        973, 974, 975, 976, 977, 978, 979, 980, 981, 982, 983
    ]

valor = turmas_ids[-1]
for i in range(426, valor + 1):
    programa(i)
