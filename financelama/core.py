# -*- coding: utf-8 -*-

import os.path
import sqlite3
from os import listdir
from os.path import isfile, join

import pandas as pd

import financelama.categories as categories
from financelama.config.drop_transactions import attributes as config_drop_attributes


class Lama:
    PATH_DB: str

    def __init__(self, path_database='lama.db'):
        self.PATH_DB = path_database

        # Create database file if not already existing
        if not os.path.isfile(self.PATH_DB):
            table = pd.DataFrame(columns=[
                'account', 'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
                'reason', 'value', 'category'])

            con = sqlite3.connect(self.PATH_DB)
            table.to_sql('transactions', con, index=False)
            con.close()

    def connect_database(self, sql_query=None):
        if sql_query==None:
            sql_query='SELECT * FROM transactions'

        con = sqlite3.connect(self.PATH_DB)
        df = pd.read_sql(sql_query, con)

        # Cast columns to correct datatypes
        if 'day' in df.columns:
            df = df.astype({'day': 'datetime64'})
        if 'value' in df.columns:
            df = df.astype({'value': 'float64'})
        if 'orderer' in df.columns:
            df = df.astype({'orderer': 'str'})
        if 'info' in df.columns:
            df = df.astype({'info': 'str'})
        if 'reason' in df.columns:
            df = df.astype({'reason': 'str'})

        return df, con

    def categorize(self, all_entries=False):
        # Load database from file
        if all_entries:
            sql_query = 'SELECT rowid, info, orderer, reason FROM transactions'
        else:
            sql_query = 'SELECT rowid, info, orderer, reason FROM transactions WHERE category IS NULL'
        df, conn = self.connect_database(sql_query)

        # Iterate over table and assign categories
        cur = conn.cursor()
        for index, row in df.iterrows():
            concat = row['orderer'] + row['reason']
            assigned_category = categories.get_category(concat)
            cur.execute('UPDATE transactions SET category = ? WHERE rowid = ?', [assigned_category, row['rowid']])

            # Print info message
            info_str = row['orderer'] + '|' + row['info'] + '|' + row['reason']
            print('[Categorize] TRANSACTION ' + info_str.ljust(80)[:80] + ' ASSIGNED TO ' + assigned_category)

        conn.commit()
        conn.close()

    def cleanup(self):
        print('Sorry, this function needs modification in order to comply with SQLite table.')
        return

        #ROADMAP check for duplicates
        #self.data = self.data.drop_duplicates()

        #ROADMAP Drop transactions with configured attributes
        #ROADMAP Smart matching of debit balances and give warning when found unmatching transaction
        # balance = 0
        # counter = 0
        # for index, row in self.data.sort_values(by=['day']).iterrows():
        #     for col, tags in config_drop_attributes.items():
        #         for t in tags:
        #             if row[col].lower().find(t.lower()) != -1:
        #                 # Print info message
        #                 msg = row['orderer'] + '|' + row['info'] + '|' + str(row['value'])
        #                 print("[Drop Transactions]" + msg)
        #
        #                 # Drop row
        #                 balance += row['value']
        #                 counter += 1
        #                 self.data.drop(index, inplace=True)
        # print('[REPORT > Drop Transactions] Total count: ' + str(counter) + ' with balance of ' + str(
        #     round(balance)) + ' EUR.')

    def read_file(self, path):
        # Read bank account from first line to determine file type (giro or debit card)
        first_line = pd.read_csv(
            path, delimiter=';', encoding='mbcs', nrows=0,
        )

        # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for special characters
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
        initial_df, conn = self.connect_database(sql_query)

        #TODO duplicates are not recognized as missing value is in csv file empty ('') and in database NULL or in python None

        # Drop rows which are already in the database (avoiding duplicates)
        df_merge = pd.merge(initial_df, csv_file, how='outer', indicator=True)
        new_data = df_merge.query('_merge == "right_only"').drop(['_merge'], 1)
        new_data.to_sql('transactions', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

        # Print info message
        msg = '[Reading file] Imported {0}/{2} transactions from {1}'
        print(msg.format(new_data.shape[0], path, csv_file.shape[0]))

    def read_folder(self, path):
        # Get file list of folder
        files = [f for f in listdir(path)
                 if isfile(join(path, f)) and f.endswith('.csv')]

        for f in files:
            # try:
            df = self.read_file(join(path, f))
            # except:
            #    print('Error while reading file: ' + f + ' in ' + path)
            #    print(sys.exc_info()[0])
