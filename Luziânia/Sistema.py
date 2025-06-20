from Contratos import Contratos
from Pessoas import Pessoas
from Turmas import Lista_Turmas, Lista_De_Pautas_Das_Turmas
import time

while True:
    Contratos.programa()
    Pessoas.programa()
    Lista_Turmas.programa()
    Lista_De_Pautas_Das_Turmas.programa()
    time.sleep(1200)
