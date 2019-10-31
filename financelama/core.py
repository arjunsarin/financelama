# -*- coding: utf-8 -*-
from typing import List

import pandas as pd

from financelama.config.lut_categories import categories as config_categories
from financelama.config.drop_transactions import attributes as config_drop_attributes

class Lama:
    data = pd.DataFrame(columns=[
        'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
        'reason', 'value', 'category'])

    def _get_category(self, identifier: str):
        for category, keywords in config_categories.items():
            # Check for each keyword
            for k in keywords:
                # Check if lower-case keyword is substring of lower-case identifier
                if identifier.lower().find(k.lower()) != -1:
                    return category
        # Default value if no category was found
        return 'other'

    def categorize(self):
        for index, row in self.data.iterrows():
            concat = row['orderer'] + row['reason']
            assigned_category = self._get_category(concat)
            self.data.set_value(index, 'category', assigned_category)
            #TODO set_value is deprecated and will be removed in a future release. Please use .at[] or .iat[] accessors instead

            if assigned_category == 'other':
                # Print info message
                info_str = row['orderer'] + '|' + row['info'] + '|' + row['reason']
                print('[Categorize] TRANSACTION ' + info_str.ljust(80)[:80] + ' ASSIGNED TO ' + assigned_category)

    def cleanup(self):
        self.data = self.data.drop_duplicates()

        # Drop transactions with configured attributes
        balance = 0
        counter = 0
        for index, row in self.data.sort_values(by=['day']).iterrows():
            for col, tags in config_drop_attributes.items():
                for t in tags:
                    if row[col].lower().find(t.lower()) != -1:
                        # Print info message
                        msg = row['orderer'] + '|' + row['info'] + '|' + str(row['value'])
                        print("[Drop Transactions]" + msg)

                        # Drop row
                        balance += row['value']
                        counter += 1
                        self.data.drop(index, inplace=True)
        print('[REPORT>Drop Transactions] Total count: ' + str(counter) + ' with balance of ' + str(round(balance)) + ' EUR.')
        #ROADMAP Smart matching of debit balances and give warning when found unmatching transaction
