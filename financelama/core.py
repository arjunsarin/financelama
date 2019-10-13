# -*- coding: utf-8 -*-

import pandas as pd

class lama:
    data = pd.DataFrame(columns=[
        'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
        'reason', 'value'])

    def readFile(self, path):
        # Read bank account from first line to determine file type (giro or debit card)
        first_line = pd.read_csv(path, delimiter=';', encoding='mbcs', nrows=0)
        
        # Read CSV file, skip Header with time and account data, MBS encoding (windows only) for special characters
        df = pd.read_csv(path, delimiter=';', encoding='mbcs', skiprows=6)
        if first_line.columns[0] == 'Kontonummer:':         ### GIRO      
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
        
        elif first_line.columns[0] == 'Kreditkarte:':   ### DEBIT CARD
            # Add columm with bank account
            df['account'] = first_line.columns[1]
            
            # Rename columns to standard definition
            df = df.rename(columns={'Wertstellung': 'day',
                                    'Buchungstext': 'info',
                                    'Beschreibung': 'orderer',
                                    'Betrag (EUR)': 'value'})
            # Drop irrelevant columns
            df = df.drop(columns=['Umsatz abgerechnet und nicht im Saldo enthalten'])
            if 'Ursprünglicher Betrag' in df.columns:
                df = df.drop(columns=['Ursprünglicher Betrag'])
            
        # Cast colums to nice data datatypes
        df['day'] = pd.to_datetime(df['day'], format='%d.%m.%Y')
        
        self.data = pd.concat([self.data, df], sort='True', ignore_index='True')

if __name__ == "__main__":
    obj = lama()
    obj.readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/1051054540.csv')
    obj.readFile('E:/Dokumente/Finanzen/Analyse/financelama-git/data/4998________2043.csv')
    
    print(obj.data)