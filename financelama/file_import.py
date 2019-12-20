import pandas as pd
import os.path

from financelama.core import Financelama

# Specification how file columns are mapped to Financelama columns (concatenates more than 1 col)
# Make empty entries for empty mapping
financelama_columns = [
    'day', 'info', 'orderer', 'reason', 'orderer_account', 'orderer_bank', 'value'
]
mapping_dkb_giro = [
    ['Wertstellung'], ['Buchungstext'], ['Auftraggeber / Begünstigter'], ['Verwendungszweck'],
    ['Kontonummer'], ['BLZ'], ['Betrag (EUR)']
]
mapping_dkb_debit = [
    ['Wertstellung'], [], ['Beschreibung'], [], [], [], ['Betrag (EUR)']
]

mapping_paypal = [
    ['Datum'], ['Hinweis'], ['Name'], ['Typ', 'Betreff'], [], [], ['Netto']
]


def _map_columns(file: pd.DataFrame, mapping: list):
    """
    Takes data frame with csv file and maps columns to Financelama columns. Not existing columns
    will be filled with an empty string.

    Parameters
    ----------
    file: pd.Dataframe
        Input dataframe read from csv file
    mapping:
        Ordered list with lists which specify which columns are to be mapped to the
    corresponding Financelama columns. See financelama_columns for expected order.

    Returns
    -------
    Financelama compatible dataframe
    """
    if len(mapping) != len(financelama_columns):
        print('Error  in _map_columns: Mismatching length  of mapping.')
        print(mapping)

    res = pd.DataFrame(columns=financelama_columns)

    for i in range(len(financelama_columns)):
        if len(mapping[i]) == 0:
            continue

        res[financelama_columns[i]] = file[mapping[i][0]]

        for j in range(1, len(mapping[i])):
            # Append file column to Financelama column
            res[financelama_columns[i]] = res[financelama_columns[i]].map(str) +\
                                            ' ' +\
                                            file[mapping[i][j]].map(str)

    return res


def _add_to_database(lama: Financelama, df: pd.DataFrame):
    """
    Adds data frame to database without creating duplicates.

    Parameters
    ----------
    lama: Financelama
        Reference to Financelama object which manages database connection.
    df: pd.Dataframe
        Transaction to be added to database

    Returns
    -------
    Integer counting how many records where actually added.
    """

    # Merge new file into existing database
    sql_query = 'SELECT ' \
                'day, info, orderer, reason, orderer_account, orderer_bank, value, ' \
                'account FROM transactions'
    initial_df, conn = lama.connect_database(sql_query)

    # TODO duplicates are not recognized as missing value is in csv file empty ('') and in
    #  database NULL or in python None

    # Drop rows which are already in the database (avoiding duplicates)
    df_merge = pd.merge(initial_df, df, how='outer', indicator=True)
    new_data = df_merge.query('_merge == "right_only"').drop(['_merge'], 1)
    new_data.to_sql('transactions', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()

    return new_data.shape[0]


def read_file_dkb(lama: Financelama, path: str):
    """
    Load CSV file into database

    The file can be either a debit card or giro export from DKB.
    Data is added to the database which is associated with that class instance.
    Potential duplicates are filtered before adding data to database.

    Parameters
    ----------
    lama : Financelama
        Reference to Financelama object which manages connection to database
    path : str
        CSV file to load
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
        df = _map_columns(csv_file, mapping_dkb_giro)

        # Add columm with bank account
        df['account'] = first_line.columns[1].split(' / ')[0]

    # DKB Debit card
    elif first_line.columns[0] == 'Kreditkarte:':
        df = _map_columns(csv_file, mapping_dkb_debit)

        # Add columm with bank account
        df['account'] = first_line.columns[1]

    # Change decimal and thousand separator in value column
    df['value'] = df['value'].apply(lambda x: x.replace('.', ''), )
    df['value'] = df['value'].apply(lambda x: x.replace(',', '.'), )

    # Parsing day to standard timestamp YYYY-MM-DD
    df['day'] = pd.to_datetime(df['day'], format='%d.%m.%Y')
    df['value'] = pd.to_numeric(df['value'])

    # Missing values are represented by '' which has to be filled by nan so
    # that the merge operation is able to filter out duplicates correctly
    df = df.fillna(value='None')

    added_records = _add_to_database(lama, df)

    # Print info message
    msg = '[Reading file] Imported {0}/{2} transactions from {1}'
    print(msg.format(added_records, path, df.shape[0]))


def read_file_paypal(lama: Financelama, path: str):
    """
    Reads CSV file exported from PayPals Web UI into Financelama database.

    Parameters
    ----------
    path : str
        CSV file to load
    lama : Financelama
        Reference to Financelama object which manages connection to database
    """

    # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for
    # special characters
    csv_file = pd.read_csv(
        path, delimiter=',', encoding='mbcs',
    )

    df = _map_columns(csv_file, mapping_paypal)

    # Add column with bank account
    df['account'] = 'PayPal'

    # Change decimal and thousand separator in value column
    df['value'] = df['value'].apply(lambda x: x.replace(',', '.'), )

    # Parsing day to standard timestamp YYYY-MM-DD
    df['day'] = pd.to_datetime(df['day'], format='%d.%m.%Y')
    df['value'] = pd.to_numeric(df['value'])

    # Missing values are represented by '' which has to be filled by nan so
    # that the merge operation is able to filter out duplicates correctly
    df = df.fillna(value='None')

    added_records = _add_to_database(lama, df)

    # Print info message
    msg = '[Reading file] Imported {0}/{2} transactions from {1}'
    print(msg.format(added_records, path, df.shape[0]))


def read_folder_dkb(lama: Financelama, path: str):
    """
    Load CSV file from folder into database

    Convenience function to load several files into database. read_file() is
    invoked for each file.

    Parameters
    ----------
    lama : Financelama
        Reference to Financelama object which manages connection to database
    path : str
         Folder containing several compatible CSV files
    """
    # Get file list of folder
    files = [f for f in os.listdir(path)
             if os.path.isfile(path + f) and f.endswith('.csv')]

    for f in files:
        # try:
        read_file_dkb(lama, (path + f))
        # except:
        #    print('Error while reading file: ' + f + ' in ' + path)
        #    print(sys.exc_info()[0])
