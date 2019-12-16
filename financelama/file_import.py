import pandas as pd
import os.path

from financelama.core import Financelama

def read_file(lama: Financelama, path: str):
    """
    Load CSV file into database

    The file can be either a debit card or giro export from DKB.
    Data is added to the database which is associated with that class instance.
    Potential duplicates are filtered before adding data to database.

    Parameters
    ----------
    path : str
        CSV file to load
    lama : Financelama
        Reference to Financelama object which manages connection to database
    """

    # Read bank account from first line to determine file type (giro or debit card)
    first_line = pd.read_csv(
        path, delimiter=';', encoding='mbcs', nrows=0,
    )

    # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for
    # special characters
    csv_file = pd.read_csv(
        path, delimiter=';', encoding='mbcs', skiprows=6,
    )

    # DKB Giro account
    if first_line.columns[0] == 'Kontonummer:':
        # Add columm with bank account
        csv_file['account'] = first_line.columns[1].split(' / ')[0]

        # Drop irrelevant columns
        csv_file = csv_file.drop(columns=['Buchungstag',
                                          'Gläubiger-ID',
                                          'Mandatsreferenz',
                                          'Kundenreferenz',
                                          'Unnamed: 11'])

        # Rename columns to standard definition
        csv_file = csv_file.rename(columns={'Wertstellung': 'day',
                                            'Buchungstext': 'info',
                                            'Auftraggeber / Begünstigter': 'orderer',
                                            'Verwendungszweck': 'reason',
                                            'Kontonummer': 'orderer_account',
                                            'BLZ': 'orderer_bank',
                                            'Betrag (EUR)': 'value'})

    # DKB Debit card
    elif first_line.columns[0] == 'Kreditkarte:':
        # Add columm with bank account
        csv_file['account'] = first_line.columns[1]

        # Create not existing columns and fill with empty string
        csv_file['info'] = ''
        csv_file['reason'] = ''

        # Rename columns to standard definition
        csv_file = csv_file.rename(columns={
            'Wertstellung': 'day',
            'Beschreibung': 'orderer',
            'Betrag (EUR)': 'value'})

        # Drop irrelevant columns
        csv_file = csv_file.drop(columns=[
            'Umsatz abgerechnet und nicht im Saldo enthalten',
            'Belegdatum',
            'Unnamed: 6'])
        if 'Ursprünglicher Betrag' in csv_file.columns:
            csv_file = csv_file.drop(columns=['Ursprünglicher Betrag'])

    # Change decimal and thousand separator in value column
    csv_file['value'] = csv_file['value'].apply(lambda x: x.replace('.', ''), )
    csv_file['value'] = csv_file['value'].apply(lambda x: x.replace(',', '.'), )

    # Parsing day to standard timestamp YYYY-MM-DD
    csv_file['day'] = pd.to_datetime(csv_file['day'], format='%d.%m.%Y')
    csv_file['value'] = pd.to_numeric(csv_file['value'])

    # Missing values are represented by '' which has to be filled by nan so
    # that the merge operation is able to filter out duplicates correctly
    csv_file = csv_file.fillna(value='None')

    # Merge new file into existing database
    sql_query = 'SELECT day, info, orderer, reason, orderer_account, orderer_bank, value, account FROM transactions'
    initial_df, conn = lama.connect_database(sql_query)

    # TODO duplicates are not recognized as missing value is in csv file empty ('') and in
    #  database NULL or in python None

    # Drop rows which are already in the database (avoiding duplicates)
    df_merge = pd.merge(initial_df, csv_file, how='outer', indicator=True)
    new_data = df_merge.query('_merge == "right_only"').drop(['_merge'], 1)
    new_data.to_sql('transactions', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()

    # Print info message
    msg = '[Reading file] Imported {0}/{2} transactions from {1}'
    print(msg.format(new_data.shape[0], path, csv_file.shape[0]))

def read_folder(lama: Financelama, path: str):
    """
    Load CSV file from folder into database

    Convenience function to load several files into database. read_file() is
    invoked for each file.

    Parameters
    ----------
    path : str
         Folder containing several compatible CSV files
    """
    # Get file list of folder
    files = [f for f in os.listdir(path)
             if os.path.isfile(path + f) and f.endswith('.csv')]

    for f in files:
        # try:
        read_file(lama, (path + f))
        # except:
        #    print('Error while reading file: ' + f + ' in ' + path)
        #    print(sys.exc_info()[0])