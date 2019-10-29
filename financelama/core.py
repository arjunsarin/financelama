# -*- coding: utf-8 -*-
from typing import List

import pandas as pd

import financelama.lut_categories


class Lama:
    data = pd.DataFrame(columns=[
        'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
        'reason', 'value', 'category'])

    def _get_category(self, identifier: str):
        for category, keywords in financelama.lut_categories.categories.items():
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
        # TODO Add duplicate removal when only minor changes are detected (manual changes in DB)
