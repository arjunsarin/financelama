# Specification how file columns are mapped to Financelama columns (concatenates more than 1 col)
# Make empty entries for empty mapping
financelama_columns = [
    'day', 'info', 'orderer', 'reason', 'orderer_account', 'orderer_bank', 'value'
]
mapping_dkb_giro = [
    ['Wertstellung'], ['Buchungstext'], ['Auftraggeber / Begünstigter'], ['Verwendungszweck'],
    ['Kontonummer'], ['BLZ'], ['Betrag (EUR)']
]
mapping_dkb_debit = [
    ['Wertstellung'], [], ['Beschreibung'], [], [], [], ['Betrag (EUR)']
]

mapping_paypal = [
    ['Datum'], ['Hinweis'], ['Name'], ['Typ', 'Betreff'], [], [], ['Netto']
]

# Transactions will be dropped if the following string is found as substring within the according
# column
dropping_keywords = {
   'orderer': ['KREDITKARTENABRECHNUNG', 'Ausgleich Kreditkarte'],
   'reason': ['umbuchung'],
}