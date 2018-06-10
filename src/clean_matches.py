import pandas as pd


def clean_matches(df, type):
    '''TODO: manual_verification'''

    if type == 'automated_safe':
        print('***Undertaking an automated safe reconciliation!***')
        df = df[df['First Score'] > 70]
        df = df[(df['First Score'] - df['Second Score'] >= 10) |
                (pd.isnull(df['Second Score']))]
    elif type == 'manual_verification':
        print('Undertaking an manual reconciliation...')
    else:
        print('No match type specified!'
              'Please specify "automated_safe" or "manual_verification"')
    return df
