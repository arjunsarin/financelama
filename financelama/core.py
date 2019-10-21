# -*- coding: utf-8 -*-

import pandas as pd


class Lama:
    data = pd.DataFrame(columns=[
        'day', 'info', 'orderer', 'orderer_account', 'orderer_bank',
        'reason', 'value'])

    def cleanup(self):
        self.data = self.data.drop_duplicates()
        # TODO Add duplicate removal when only minor changes are detected (manual changes in DB)
