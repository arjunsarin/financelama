# -*- coding: utf-8 -*-

import pandas as pd
from os import listdir
from os.path import isfile, join

import sys

def read_file(df: pd.DataFrame, path) -> pd.DataFrame:
    # Read bank account from first line to determine file type (giro or debit card)
    first_line = pd.read_csv(
        path, delimiter=';', encoding='mbcs', nrows=0,
    )

    # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for special characters
    csv_file = pd.read_csv(
        path, delimiter=';', encoding='mbcs', skiprows=6,
    )

    if first_line.columns[0] == 'Kontonummer:':  ### GIRO
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

    elif first_line.columns[0] == 'Kreditkarte:':  ### DEBIT CARD
        # Add columm with bank account
        csv_file['account'] = first_line.columns[1]

        # Rename columns to standard definition
        csv_file = csv_file.rename(columns={
            'Wertstellung': 'day',
            'Buchungstext': 'info',
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

    # Cast colums to nice datatypes
    csv_file['day'] = pd.to_datetime(csv_file['day'], format='%d.%m.%Y')
    csv_file['value'] = pd.to_numeric(csv_file['value'])

    return pd.concat([df, csv_file], sort='True', ignore_index='True')


def read_folder(df: pd.DataFrame, path) -> pd.DataFrame:
    # Get file list of folder
    files = [f for f in listdir(path)
             if isfile(join(path, f)) and f.endswith('.csv')]

    for f in files:
        try:
            df = read_file(df, join(path, f))
        except:
            print('Error while reading file: ' + f + ' in ' + path)
            print(sys.exc_info()[0])

    return df
