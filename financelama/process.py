from financelama.core import Financelama

categories = {
    'supermarket': ['rewe', 'coop', 'edeka', 'lidl', 'netto', 'norma', 'frukt', 'ica'],
    'rent': ['miete', 'miete', 'wohnung', 'etagenbeitrag'],
    'entertainment': ['netflix', 'spotify', 'ticket', 'konzert', 'museum', 'filmstaden'],
    'restaurant': ['restaurant', 'restaurant', 'bar', 'cafe', 'qstockholm'],
    'traffic': ['db', 'deutschebahn', 'train', 'deutsche bahn'],
    'income': ['gehalt', 'lohn', 'stipendium'],
    'cash': ['bankomat']
}


def _get_category(identifier: str) -> str:
    """
    Finds suitable category for given identifier from look up table. If no
    matching category is found, 'other' as fallback will be returned.

    Parameters
    ----------
    identifier : str
        String which is used for finding category

    Returns
    -------
    category : str
    """
    for category, keywords in categories.items():
        # Check for each keyword
        for k in keywords:
            # Check if lower-case keyword is substring of lower-case identifier
            if identifier.lower().find(k.lower()) != -1:
                return category
    # Default value if no category was found
    return 'other'


def categorize(lama: Financelama, all_entries=False):
    """
    Add categories to rows in database

    Parameters
    ----------
    lama : Financelama
        References to Financelama object for database access.
    all_entries : bool, optional
        Assigns categories to ALL rows neglecting existing assignments
    """

    # Load database from file
    if all_entries:
        sql_query = 'SELECT rowid, info, orderer, reason FROM transactions'
    else:
        sql_query = 'SELECT rowid, info, orderer, reason FROM transactions WHERE category IS NULL'
    df, conn = lama.connect_database(sql_query)

    # Iterate over table and assign categories
    cur = conn.cursor()
    for index, row in df.iterrows():
        concat = row['orderer'] + row['reason']
        assigned_category = _get_category(concat)
        cur.execute('UPDATE transactions SET category = ? WHERE _ROWID_ = ?',
                    [assigned_category, row['rowid']])

        # Print info message
        info_str = row['orderer'] + '|' + row['info'] + '|' + row['reason']
        print('[Categorize] TRANSACTION ' + info_str.ljust(80)[
                                            :80] + ' ASSIGNED TO ' + assigned_category)

    conn.commit()
    conn.close()


def cleanup():
    print('Sorry, this function needs modification in order to comply with SQLite table.')
    return

    # ROADMAP check for duplicates
    # self.data = self.data.drop_duplicates()

    # Transactions will be dropped if the following string is found as substring within the according column
    #attributes = {
    #    'orderer': ['KREDITKARTENABRECHNUNG', 'Ausgleich Kreditkarte'],
    #}

    # ROADMAP Drop transactions with configured attributes
    # ROADMAP Smart matching of debit balances and give warning when found unmatching transaction
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


def modify_report(lama: Financelama, report_name: str,
                  list_of_rowids=None,
                  list_of_ranges=None):
    """
    Modify report column in database for specified rows

    Updates report column in database for all rows specified by rowid. All
    transactions within the same report are handled as a single expense,
    for example holiday expenses can be summarized into one report.
    Note: The user has to make sure that new report name isn't used already.

    Parameters
    ----------
    lama : Financelama
        References to Financelama object for database access.
    report_name : str
        Name of report
    list_of_rowids : list of ints, optional
        RowIds to update
    list_of_ranges : list of int touples, optional
        Range of rowids will be updated with report_name. Both values are included.

    """
    conn = lama.connect_database()[1]
    cur = conn.cursor()

    counter = 0
    if list_of_rowids is not None:
        for i in list_of_rowids:
            cur.execute('UPDATE transactions SET report =  ? WHERE _ROWID_ = ?',
                        [report_name, i])
            counter += 1

    if list_of_ranges is not None:
        for i in list_of_ranges:
            row_ids = list(range(i[0], i[1] + 1))
            arg = list(zip([report_name] * len(row_ids), row_ids))
            cur.executemany('UPDATE transactions SET report =  ? WHERE _ROWID_ = ?', arg)
            counter += len(row_ids)

    conn.commit()
    conn.close()