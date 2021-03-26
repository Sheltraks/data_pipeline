from typing import Generator, Dict, List, Any

'''
soll große Dateien auslesen können
ETL - Extract, Transform, Load - Pipeline
 alles 3 eigentlich ne pipeline
'''

# das erste was eine pipeline macht ist eine Quelle einlesen
# wird von load_data eingelesen
def read_large_dataset(file_name: str) -> Generator:
    with open(file_name) as f:  # with? indent?  - f ist der "handler"/Zeiger auf die Datei
        # print(f.read())  # ungünstig weils die ganze Datei einliest - wollen wir ja nicht
        for line in f:  # liest(gibt) Zeilenweise aus
            yield line  # yield ..?

def split_lines(file_generator: Generator):
    print("Split lines Gen: ", type(file_generator))
    '''
    spalte Datei in Zeilen, kommasept    
    '''
    '''
    for line in file_generator:
        print(lie.split(","))  # 
    restul = []
    for line in file_generator:
        result.append(line.split(","))
    funktioniert zwar, nutzt/"consumed" aber den Generator
    '''
    # erstellt über alle items (line) in file_generator,
    # stript sie und splittet sie nach Komma in eine Liste
    result = (line.strip().split(',')  for line in file_generator)
    return result

def dictifiy(list_generator: Generator):
    cols = next(list_generator)  # gibt eine Zeile aus
    print(cols)
    # verknüpft Zeile cols mit dem restlichen listen zu einem dicitionary
    ausgabe = (dict(zip(cols, data))  for data in list_generator)  # setzt Zeile und Head zusammen?
    return ausgabe

def load_data(file_name: str):
    # print(file_name)  # testausgabe
    file_generator = read_large_dataset(file_name)  # führt das einlesen aus
    # print(next(file_generator))  # über den Datensatz iterieren ohne es in den Ram zu laden
    split_generator = split_lines(file_generator)  # Generator übergeben an Listengenerator
    dictionar = dictifiy(split_generator)
    resultat = dictionar
    return resultat
