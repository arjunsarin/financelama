from financelama.core import Financelama

import timeit

from financelama.file_import import read_file_dkb, read_folder_dkb, read_file_paypal
from financelama.process import categorize, modify_report
from financelama.evaluation import evaluate
from financelama.visual import start_dashboard


def benchmark(function):
    print(timeit.timeit(function, number=1))


# Create lama object with dataframe as data property
lama = Financelama()

# Read files
# obj.read_file('E:/Dokumente/Finanzen/Analyse/financelama-git/data/1051054540(3).csv')
# Lots of data here
# read_folder_dkb(lama, 'data/dkb/')
# read_file_paypal(lama, 'E:/Dokumente/Finanzen/Analyse/financelama-git/data/paypal_download.CSV')

# categorize(lama)

# modify_report(lama, 'urlaub', list_of_rowids={1, 2, 3})
# modify_report(lama, 'urlaub', list_of_ranges={(617, 620)})


# Start DASH web application
start_dashboard(evaluate(lama))

# os.remove('lama.db')
