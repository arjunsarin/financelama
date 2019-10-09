import pandas

def readFile(path):
    # Read CSV file from DKB banking
    # Skip Header with time and account data, MBS encoding (windows only) for special characters
    df = pandas.read_csv(path, delimiter=';', encoding='mbcs', skiprows=6)

    # Rename columns to standard definition
    df = df.rename(columns={'Wertstellung': 'day',
                            'Buchungstext': 'info',
                            'Auftraggeber / Begünstigter': 'orderer',
                            'Verwendungszweck': 'reason',
                            'Kontonummer': 'orderer_account',
                            'BLZ': 'orderer_bank',
                            'Betrag (EUR)': 'value'})

    # Drop irrelevant columns
    df = df.drop(columns=['Buchungstag', 'Gläubiger-ID', 'Mandatsreferenz', 'Kundenreferenz', 'Unnamed: 11'])

    # Cast colums to nice data datatypes
    df['day'] = pandas.to_datetime(df['day'], format='%d.%m.%Y')

    # Read bank account from first line, filter for IBAN and add to data frame
    act_df = pandas.read_csv( path, delimiter=';|"| / ', engine='python', encoding='mbcs', nrows=0).filter(regex='^[A-Z]{2}[0-9]{1,34}$')
    df['account'] = act_df.columns[0]

    return df


table = readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/1051054540.csv')
print(table)

# print(account.columns)
