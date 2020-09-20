"""
Read and process CSV

Developer: Stanislav Ermokhin
"""

import pandas as pd

YEAR = 2018
DIC = {'Assets': {'Cash & Short Term Investments': {'Cash Only': '',
                                                    'Short-Term Investments': ''},
                  'Total Accounts Receivable': {'Accounts Receivables, Net': {'Accounts Receivables, Gross': '',
                                                                              'Bad Debt/Doubtful Accounts': ''},
                                                'Other Receivables': ''},
                  'Inventories': {'Finished Goods': ''},
                  'Other Current Assets': {'Miscellaneous Current Assets': ''},
                  'Total Current Assets': '',
                  'Net Property, Plant & Equipment': {'Property, Plant & Equipment - Gross': {'Buildings': '',
                                                                                              'Construction in Progress': '',
                                                                                              'Leases': '',
                                                                                              'Computer Software and Equipment': '',
                                                                                              'Other Property, Plant & Equipment': ''},
                                                      'Accumulated Depreciation': ''},
                  'Total Investments and Advances': {'LT Investment - Affiliate Companies': '',
                                                     'Other Long-Term Investments': ''},
                  'Long-Term Note Receivable': '',
                  'Intangible Assets': {'Net Goodwill': '',
                                        'Net Other Intangibles': ''},
                  'Other Assets': {'Tangible Other Assets': ''},
                  'Total Assets': ''},
       'Liabilities & Shareholders\' Equity': {'ST Debt & Current Portion LT Debt': {'Short Term Debt': '',
                                                                                     'Current Portion of Long Term Debt': ''},
                                               'Accounts Payable': '',
                                               'Income Tax Payable': '',
                                               'Other Current Liabilities': {'Accrued Payroll': '',
                                                                             'Miscellaneous Current Liabilities': ''},
                                               'Total Current Liabilities': '',
                                               'Long-Term Debt': {'Long-Term Debt excl. Capitalized Leases': {'Non-Convertible Debt': ''},
                                                                  'Capitalized Lease Obligations': ''},
                                               'Deferred Taxes': {'Deferred Taxes - Credit': '',
                                                                  'Deferred Taxes - Debit': ''},
                                               'Other Liabilities': {'Other Liabilities (excl. Deferred Income)': '',
                                                                     'Deferred Income': ''},
                                               'Total Liabilities': '',
                                               'Common Equity (Total)': {'Common Stock Par/Carry Value': '',
                                                                         'Additional Paid-In Capital/Capital Surplus': '',
                                                                         'Retained Earnings': '',
                                                                         'Cumulative Translation Adjustment/Unrealized For. Exch. Gain': '',
                                                                         'Unrealized Gain/Loss Marketable Securities': '',
                                                                         'Other Appropriated Reserves': ''},
                                               'Total Shareholders\' Equity': '',
                                               'Total Equity': '',
                                               'Liabilities & Shareholders\' Equity': ''}}


def get(filename):
    df = pd.read_csv(filename, sep=';')
    # df.to_excel('super.xlsx')

    return df[['name', str(YEAR)]]


def condition_check(str_row):
    str_row = str_row.replace(',', '')

    if str_row == '-':
        return None

    if '(' in str_row and ')' in str_row:
        str_row = str_row.replace('(', '-').strip(')')

    if '%' in str_row:
        str_row = str(float(str_row.strip('%')) / 100)

    if 'M' in str_row:
        str_row = str(float(str_row.strip('M')) * (10**6))

    if 'B' in str_row:
        str_row = str(float(str_row.strip('B')) * (10**9))

    return str_row


def process(data_frame):
    return data_frame[str(YEAR)].astype(str).map(condition_check, None).astype(float) / (10**6)


def main(filename):
    df = get(filename)
    new_2018 = process(df)

    df_to_return = pd.DataFrame({'Metric': df['name'],
                                 str(YEAR): new_2018})

    df_to_return.to_excel('processed_data.xlsx', sheet_name='Data')

    return df_to_return


def get_key(value,
            key_to_find,
            obj):
    for key in obj:
        if isinstance(obj[key], str):
            if key == key_to_find:
                obj.update({key_to_find: value})
            else:
                continue
        elif isinstance(obj[key], dict):
            get_key(value=value,
                    key_to_find=key_to_find,
                    obj=obj[key])


def process_output(filename):
    new_df = main(filename).set_index('Metric').to_dict()[str(YEAR)]
    for name in new_df:
        get_key(value=new_df[name],
                key_to_find=name,
                obj=DIC)

    d = dict()
    for key1 in DIC:
        for key2 in DIC[key1]:
            if isinstance(DIC[key1][key2], dict):
                for key3 in DIC[key1][key2]:
                    if isinstance(DIC[key1][key2][key3], dict):
                        for key4 in DIC[key1][key2][key3]:
                            d[(key1, key2, key3, key4)] = DIC[key1][key2][key3][key4]
                    else:
                        d[(key1, key2, key3, '')] = DIC[key1][key2][key3]
            else:
                d[(key1, key2, '', '')] = DIC[key1][key2]

    mux = pd.MultiIndex.from_tuples(d.keys())
    df = pd.DataFrame({'Amount, mil. USD': list(d.values())},
                      index=mux).dropna(subset=['Amount, mil. USD'])
    df.to_excel('super.xlsx')

    return df


print(process_output('bGOOGL.csv'))
