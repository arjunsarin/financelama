# -*- coding: utf-8 -*-

import pandas as pd
from os import listdir
from os.path import isfile, join


class lama:
    data = pd.DataFrame(columns=[
        'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
        'reason', 'value'])

    def read_file(self, path):
        # Read bank account from first line to determine file type (giro or debit card)
        first_line = pd.read_csv(
            path, delimiter=';', encoding='mbcs', nrows=0,
        )

        # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for special characters
        df = pd.read_csv(
            path, delimiter=';', encoding='mbcs', skiprows=6,
        )

        if first_line.columns[0] == 'Kontonummer:':  ### GIRO
            # Add columm with bank account
            df['account'] = first_line.columns[1].split(' / ')[0]

            # Drop irrelevant columns
            df = df.drop(columns=['Buchungstag',
                                  'Gläubiger-ID',
                                  'Mandatsreferenz',
                                  'Kundenreferenz',
                                  'Unnamed: 11'])

            # Rename columns to standard definition
            df = df.rename(columns={'Wertstellung': 'day',
                                    'Buchungstext': 'info',
                                    'Auftraggeber / Begünstigter': 'orderer',
                                    'Verwendungszweck': 'reason',
                                    'Kontonummer': 'orderer_account',
                                    'BLZ': 'orderer_bank',
                                    'Betrag (EUR)': 'value'})

        elif first_line.columns[0] == 'Kreditkarte:':  ### DEBIT CARD
            # Add columm with bank account
            df['account'] = first_line.columns[1]

            # Rename columns to standard definition
            df = df.rename(columns={
                'Wertstellung': 'day',
                'Buchungstext': 'info',
                'Beschreibung': 'orderer',
                'Betrag (EUR)': 'value'})

            # Drop irrelevant columns
            df = df.drop(columns=[
                'Umsatz abgerechnet und nicht im Saldo enthalten',
                'Belegdatum',
                'Unnamed: 6'])
            if 'Ursprünglicher Betrag' in df.columns:
                df = df.drop(columns=['Ursprünglicher Betrag'])

        # Change decimal and thousand separator in value column
        df['value'] = df['value'].apply(lambda x: x.replace('.', ''), )
        df['value'] = df['value'].apply(lambda x: x.replace(',', '.'), )

        # Cast colums to nice datatypes
        df['day'] = pd.to_datetime(df['day'], format='%d.%m.%Y')
        df['value'] = pd.to_numeric(df['value'])

        self.data = pd.concat([self.data, df], sort='True', ignore_index='True')
        self.cleanup()

    def read_folder(self, path):
        # Get file list of folder
        files = [f for f in listdir(path)
                 if isfile(join(path, f)) and f.endswith('.csv')]

        for f in files:
            try:
                self.read_file(join(path, f))
            except:
                print('Error while reading file: ' + f)

    def cleanup(self):
        self.data = self.data.drop_duplicates()
        # TODO Add duplicate removal when only minor changes are detected (manual changes in DB)


def main():
    obj = lama()
    # obj.readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/1051054540.csv')
    # obj.readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/1051054540(3).csv')   # Lots of data here
    # obj.readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/4998________2043.csv')
    obj.read_folder('E:/Dokumente/Finanzen/Analyse/financelama-git/data/')

    obj.cleanup()

    obj.data.to_csv(path_or_buf='E:/Dokumente/Finanzen/Analyse/financelama-git/financelama/OutputDB.csv')

    return obj


if __name__ == "__main__":
    main()
