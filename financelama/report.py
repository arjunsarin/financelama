from financelama.core import Financelama


def evaluate(lama: Financelama):
    """
    Evaluates database in preparation of analysis.

    Replaces all reported transactions with single aggregated new row.

    Returns
    -------
    pandas.DataFrame
        Evaluated data ready to be processed of analysis module (e.g. visual)
    """
    # Get total data frame
    df, conn = lama.connect_database('SELECT * FROM transactions')
    cur = conn.cursor()

    # Evaluate reports
    cur.execute('SELECT DISTINCT report FROM transactions')
    report_names = cur.fetchall()

    for name in report_names:
        if name[0] is None:
            continue

        # Aggregate data from database
        cur.execute('SELECT SUM(value) FROM transactions WHERE report=?', name)
        sum = cur.fetchone()[0]
        cur.execute('SELECT MIN(day) FROM transactions WHERE report=?', name)
        date = cur.fetchone()[0]

        # Add row with aggregated exense
        df.append({'day': date,
                   'reason': name[0],
                   'value': sum,
                   'info': 'REPORTED'},
                  ignore_index=True)

    # Remove all reported transaction from dataframe
    df = df[df.report == 'None']

    conn.close()

    return df
