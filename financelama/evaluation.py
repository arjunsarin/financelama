from financelama.core import Financelama
import pandas as pd


def _eval_report(df: pd.DataFrame, conn):
    """
    Replaces all reported transactions with single aggregated new row.

    Parameters
    ----------
    df: pd.Dataframe
        Initial dataframe
    conn:
        Connection to SQLite3 database.

    Returns
    -------
    Dataframe where transactions as part of reports are replaced by their aggregated evaluation.
    """
    cur = conn.cursor()

    # Evaluate reports
    cur.execute('SELECT DISTINCT report FROM transactions')
    report_names = cur.fetchall()

    for name in report_names:
        if name[0] is None:
            continue

        # Aggregate data from database
        cur.execute('SELECT SUM(value) FROM transactions WHERE report=?', name)
        aggregated_sum = cur.fetchone()[0]
        cur.execute('SELECT MIN(day) FROM transactions WHERE report=?', name)
        date = cur.fetchone()[0]

        # Add row with aggregated exense
        df.append({'day': date,
                   'reason': name[0],
                   'value': aggregated_sum,
                   'info': 'REPORTED'},
                  ignore_index=True)

    # Remove all reported transaction from dataframe
    df = df[df.report == 'None']

    return df


def evaluate(lama: Financelama):
    """
    Evaluates database in preparation of analysis, e.g. calls all existing evaluation function.

    Returns
    -------
    pandas.DataFrame
        Evaluated data ready to be processed of visualization module (e.g. visual)
    """
    # Get total data frame
    df, conn = lama.connect_database('SELECT * FROM transactions')

    _eval_report(df, conn)

    conn.close()

    return df
