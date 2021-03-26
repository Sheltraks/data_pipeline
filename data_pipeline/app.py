from pipeline import load_data


# app oder auch main

def main():
    data_path = '../data/techcrunch.csv'  # Angabe des Datenpfades
    resultat = load_data(data_path)
    print("Resultat: ", resultat)

if __name__ == '__main__':
    main()