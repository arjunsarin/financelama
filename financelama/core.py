# -*- coding: utf-8 -*-

import os.path
import sqlite3

import pandas as pd


class Financelama:
    """
    Manages database and provides access for all other functions

    Attributes
    ----------
    PATH_DB: str
        Path to SQLite database file.

    Methods
    -------
    connect_database(sql_query=None)
        Connect to database and casts columns to correct datatypes.
    """
    PATH_DB: str

    def __init__(self, path_database='lama.db'):
        """ Check if database file already exists and create with all columns if not.

        Parameters
        ----------
        path_database : str, optional
            Path to SQLite database (either existing or not). Default: 'lama.db'
        """
        self.PATH_DB = path_database

        # Create database file if not already existing
        if not os.path.isfile(self.PATH_DB):
            table = pd.DataFrame(columns=[
                'account', 'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
                'reason', 'value', 'category', 'report'])

            con = sqlite3.connect(self.PATH_DB)
            table.to_sql('transactions', con, index=False)
            con.close()

    def connect_database(self, sql_query: str = None):
        """ Connect to database and casts columns to correct datatypes.

        Parameters
        ----------
        sql_query : str, optional
            Optional SQL query to run against database. If not given, all
            columns from table transactions are retrieved.

        Returns
        -------
        Both, a pandas.Dataframe and the database connection are returned as
        touple in that very order.
        """
        if sql_query is None:
            sql_query = 'SELECT * FROM transactions'

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
        if 'category' in df.columns:
            df = df.astype({'category': 'str'})
        if 'report' in df.columns:
            df = df.astype({'report': 'str'})

        return df, con
